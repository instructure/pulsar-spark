/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pulsar

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats

case class TopicState(internalStat: PersistentTopicInternalStats,
                      actualLedgerId: Long,
                      actualEntryId: Long)

trait ForwardStrategy {
  def forward(topics: Map[String, TopicState]): Map[String, Long]
}

/**
 * Simple forward strategy, which forwards every topic evenly, not
 * taking actual backlog sizes into account. Might waste bandwidth
 * when the backlog of the topic is smaller than the calculated value
 * for that topic.
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to
 *                                      forward. Will forward every topic
 *                                      by dividing this with the number of
 *                                      topics.
 */
class LinearForwardStrategy(maxEntriesAltogetherToForward: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] =
    topics
      .map{ keyValue => keyValue._1 -> (maxEntriesAltogetherToForward / topics.size) }
}

/**
 * Forward strategy which sorts the topics by their backlog size starting
 * with the largest, and forwards topics starting from the beginning of
 * this list as the maximum entries parameter allows (taking into account
 * the number entries that need to be added anyway if
 * @param additionalEntriesPerTopic is set).
 *
 * If the maximum entries to forward is `100`, topics will be forwarded
 * like this (provided there is no minimum entry number specified:
 * | topic name | backlog size | forward amount |
 * |------------|--------------|----------------|
 * |topic-1     |           60 |             60 |
 * |topic-2     |           50 |             40 |
 * |topic-3     |           40 |              0 |
 *
 * If there is a minimum entry number if specified, then every topic will be
 * forwarded by that value in addition to this (taking the backlog size of
 * the topic into account so that bandwidth is not wasted). Given maximum
 * entries is `100`, minimum entries is `10`, topics will be forwarded like
 * this:
 *
 * | topic name | backlog size | forward amount |
 * |------------|--------------|----------------|
 * |topic-1     |           60 |   10 + 50 = 60 |
 * |topic-2     |           50 |   10 + 30 = 30 |
 * |topic-3     |           40 |    10 + 0 = 10 |
 *
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to forward.
 *                                      Individual topics forward values will sum
 *                                      up to this value.
 * @param additionalEntriesPerTopic All topics will be forwarded by this value. The goal
 *                                  of this parameter is to ensure that topics with a very
 *                                  small backlog are also forwarded with a given minimal
 *                                  value.
 */
class LargeFirstForwardStrategy(maxEntriesAltogetherToForward: Long,
                                additionalEntriesPerTopic: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] = {
    val topicBacklogs = topics
      .map{
        keyValue => {
          val topicName = keyValue._1
          val topicStat = keyValue._2.internalStat
          val ledgerId = keyValue._2.actualLedgerId
          val entryId = keyValue._2.actualEntryId
          (topicName, TopicInternalStatsUtils.numOfMessagesAfter(topicStat, ledgerId, entryId))
        }}
      .toList
      .sortBy(_._2)
      .reverse

    var quota = Math.max(maxEntriesAltogetherToForward - additionalEntriesPerTopic * topics.size, 0)

    val result = for ((topic, topicBacklogSize) <- topicBacklogs) yield {
      val resultEntry = topic ->
        (additionalEntriesPerTopic +
          Math.max(Math.min(quota, topicBacklogSize - additionalEntriesPerTopic), 0))
      quota -= (topicBacklogSize - additionalEntriesPerTopic)
      resultEntry
    }

    result.toMap
  }
}

/**
 * This forward strategy will forward individual topic backlogs based on
 * their size proportional to the size of the overall backlog (considering
 * all topics).
 *
 * If the maximum entries to forward is `100`, topics will be forwarded
 * like this (provided there is no minimum entry number specified:
 * | topic name | backlog size | forward amount           |
 * |------------|--------------|--------------------------|
 * |topic-1     |           60 | 100*(60/(60+50+40)) = 40 |
 * |topic-2     |           50 | 100*(50/(60+50+40)) = 33 |
 * |topic-3     |           40 | 100*(40/(60+50+40)) = 27 |
 *
 * If there is a minimum entry number if specified, then every topic will be
 * forwarded by that value in addition to this (taking the backlog size of
 * the topic into account so that bandwidth is not wasted).
 * Given maximum entries is `100`, minimum entries is `10`, topics will be
 * forwarded like this:
 *
 * | topic name | backlog size |             forward amount |
 * |------------|--------------|----------------------------|
 * |topic-1     |           60 | 10+70*(60/(60+50+40)) = 38 |
 * |topic-2     |           50 | 10+70*(50/(60+50+40)) = 33 |
 * |topic-3     |           40 | 10+70*(40/(60+50+40)) = 29 |
 *
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to forward.
 *                                      Individual topics forward values will sum
 *                                      up to this value.
 * @param additionalEntriesPerTopic All topics will be forwarded by this value. The goal
 *                                  of this parameter is to ensure that topics with a very
 *                                  small backlog are also forwarded with a given minimal
 *                                  value.
 */
class ProportionalForwardStrategy(maxEntriesAltogetherToForward: Long,
                                  additionalEntriesPerTopic: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] = {
    val topicBacklogs = topics
      .map{
        keyValue => {
          val topicName = keyValue._1
          val topicStat = keyValue._2.internalStat
          val ledgerId = keyValue._2.actualLedgerId
          val entryId = keyValue._2.actualEntryId
          (topicName, TopicInternalStatsUtils.numOfMessagesAfter(topicStat, ledgerId, entryId))
        }}
      .toList

    val completeBacklogSize = topicBacklogs
      .map(_._2)
      .sum

    val quota = Math.max(maxEntriesAltogetherToForward - additionalEntriesPerTopic * topics.size, 0)

    topicBacklogs.map {
      case (topicName: String, backLog: Long) =>
        val topicBacklogCoefficient = backLog.toFloat / completeBacklogSize.toFloat
        topicName -> (additionalEntriesPerTopic + (quota * topicBacklogCoefficient).toLong)
    }.toMap
  }
}

object TopicInternalStatsUtils {

  def forwardMessageId(stats: PersistentTopicInternalStats,
                       startLedgerId: Long,
                       startEntryId: Long,
                       forwardByEntryCount: Long): (Long, Long) = {
    val ledgers = stats.ledgers.asScala.toList
    if (ledgers.isEmpty) {
      // If there are no ledger info, stay at current ID
      (startLedgerId, startEntryId)
    } else {
      // Find the start ledger and entry ID
      var actualLedgerIndex = if (ledgers.exists(_.ledgerId == startLedgerId)) {
        ledgers.indexWhere(_.ledgerId == startLedgerId)
      } else {
        0
      }

      var actualEntryId = Math.max(startEntryId, 0)
      var entriesToSkip = forwardByEntryCount

      while (entriesToSkip > 0) {
        val currentLedger = ledgers(actualLedgerIndex)
        val remainingElementsInCurrentLedger = currentLedger.entries - actualEntryId

        if (entriesToSkip <= remainingElementsInCurrentLedger) {
          actualEntryId += entriesToSkip
          entriesToSkip = 0
        } else if ((remainingElementsInCurrentLedger < entriesToSkip)
          && (actualLedgerIndex < (ledgers.size-1))) {
          // Moving onto the next ledger
          entriesToSkip -= remainingElementsInCurrentLedger
          actualLedgerIndex += 1
          actualEntryId = 0
        } else {
          // This is the last ledger
          val entriesInLastLedger = ledgers(actualLedgerIndex).entries
          actualEntryId = Math.min(entriesToSkip, entriesInLastLedger)
          entriesToSkip = 0
        }
      }

      (ledgers(actualLedgerIndex).ledgerId, actualEntryId)
    }
  }

  def numOfMessagesUntil(stats: PersistentTopicInternalStats,
                         ledgerId: Long,
                         entryId: Long): Long = {
    val messageCountBeforeStartLedger = stats.ledgers
      .asScala
      .filter(_.ledgerId < ledgerId)
      .map(_.entries)
      .sum
    entryId + messageCountBeforeStartLedger
  }

  def numOfMessagesAfter(stats: PersistentTopicInternalStats,
                         ledgerId: Long,
                         entryId: Long): Long = {
    val messageCountIncludingCurrentLedger = stats.ledgers
      .asScala
      .filter(_.ledgerId >= ledgerId)
      .map(_.entries)
      .sum
    messageCountIncludingCurrentLedger - entryId
  }

  def entryCount(stats: PersistentTopicInternalStats): Long = {
    stats.ledgers.asScala.map(_.entries).sum
  }

}
