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

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.pulsar.shade.org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.pulsar.shade.org.apache.avro.Conversions.DecimalConversion
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.pulsar.shade.org.apache.avro.Schema.Type._
import org.apache.pulsar.shade.org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.pulsar.shade.org.apache.avro.util.Utf8

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class PulsarDeserializer(schemaInfo: SchemaInfo, parsedOptions: JSONOptionsInRead) {

  private lazy val decimalConversions = new DecimalConversion()

  val rootDataType: DataType = SchemaUtils.si2SqlType(schemaInfo).dataType

  import SchemaUtils._

  def deserialize(message: Message[_]): SpecificInternalRow = converter(message)

  private val converter: Message[_] => SpecificInternalRow = {

    schemaInfo.getType match {
      case SchemaType.AVRO =>
        val st = rootDataType.asInstanceOf[StructType]
        val resultRow = new SpecificInternalRow(
          st.map(_.dataType) ++ metaDataFields.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)
        val avroSchema =
          new Schema.Parser().parse(new String(schemaInfo.getSchema, StandardCharsets.UTF_8))
        val writer = getRecordWriter(avroSchema, st, Nil)
        (msg: Message[_]) =>
          {
            val value = msg.getValue
            writer(fieldUpdater, value.asInstanceOf[GenericAvroRecord].getAvroRecord)
            writeMetadataFields(msg, resultRow)
            resultRow
          }

      case SchemaType.JSON =>
        val st = rootDataType.asInstanceOf[StructType]
        val resultRow = new SpecificInternalRow(
          st.map(_.dataType) ++ metaDataFields.map(_.dataType))
        val createParser = CreateJacksonParser.string _
        val rawParser = new JacksonRecordParser(rootDataType, parsedOptions)
        val parser = new FailureSafeRecordParser[String](
          (input, record) => rawParser.parse(input, createParser, UTF8String.fromString, record),
          parsedOptions.parseMode,
          st)
        (msg: Message[_]) =>
          {
            val value = msg.getData
            parser.parse(new String(value, java.nio.charset.StandardCharsets.UTF_8), resultRow)
            writeMetadataFields(msg, resultRow)
            resultRow
          }

      case _ => // AtomicTypes
        val tmpRow = new SpecificInternalRow(Seq(rootDataType) ++ metaDataFields.map(_.dataType))
        val fieldUpdater = new RowUpdater(tmpRow)
        val writer = newAtomicWriter(rootDataType)
        (msg: Message[_]) =>
          {
            val value = msg.getValue
            writer(fieldUpdater, 0, value)
            writeMetadataFields(msg, tmpRow)
            tmpRow
          }
    }
  }

  def writeMetadataFields(message: Message[_], row: SpecificInternalRow): Unit = {
    val metaStartIdx = row.numFields - 5
    // key
    if (message.hasKey) {
      row.update(metaStartIdx, message.getKeyBytes)
    } else {
      row.setNullAt(metaStartIdx)
    }
    // topic
    row.update(metaStartIdx + 1, UTF8String.fromString(message.getTopicName))
    // messageId
    row.update(metaStartIdx + 2, message.getMessageId.toByteArray)
    // publish time
    row.setLong(
      metaStartIdx + 3,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getPublishTime)))
    // event time
    if (message.getEventTime > 0) {
      row.setLong(
        metaStartIdx + 4,
        DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getEventTime)))
    } else {
      row.setNullAt(metaStartIdx + 4)
    }
  }

  private def newAtomicWriter(dataType: DataType): (RowUpdater, Int, Any) => Unit =
    dataType match {
      case ByteType =>
        (updater, ordinal, value) =>
          updater.setByte(ordinal, value.asInstanceOf[Byte])

      case BooleanType =>
        (updater, ordinal, value) =>
          updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case IntegerType =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case DateType =>
        (updater, ordinal, value) =>
          updater.setInt(
            ordinal, DateTimeUtils.microsToDays(value.asInstanceOf[Date].getTime * 1000L,
              parsedOptions.zoneId))

      case LongType =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long])

      case TimestampType =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp]))

      case FloatType =>
        (updater, ordinal, value) =>
          updater.setFloat(ordinal, value.asInstanceOf[Float])

      case DoubleType =>
        (updater, ordinal, value) =>
          updater.setDouble(ordinal, value.asInstanceOf[Double])

      case ShortType =>
        (updater, ordinal, value) =>
          updater.setShort(ordinal, value.asInstanceOf[Short])

      case StringType =>
        (updater, ordinal, value) =>
          val str = UTF8String.fromString(value.asInstanceOf[String])
          updater.set(ordinal, str)

      case BinaryType =>
        (updater, ordinal, value) =>
          val bytes = value match {
            case b: ByteBuffer =>
              val bytes = new Array[Byte](b.remaining)
              b.get(bytes)
              bytes
            case b: Array[Byte] => b
            case other => throw new RuntimeException(s"$other is not a valid avro binary.")
          }
          updater.set(ordinal, bytes)

      case tpe =>
        throw new NotImplementedError(s"$tpe is not supported for the moment")
    }

  private def newWriter(
      avroType: Schema,
      catalystType: DataType,
      path: List[String]): (CatalystDataUpdater, Int, Any) => Unit =
    (avroType.getType, catalystType) match {
      case (NULL, NullType) =>
        (updater, ordinal, _) =>
          updater.setNullAt(ordinal)

      case (BOOLEAN, BooleanType) =>
        (updater, ordinal, value) =>
          updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (INT, IntegerType) =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, DateType) =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case (LONG, LongType) =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, TimestampType) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (updater, ordinal, value) =>
              updater.setLong(ordinal, value.asInstanceOf[Long] * 1000)
          case _: TimestampMicros =>
            (updater, ordinal, value) =>
              updater.setLong(ordinal, value.asInstanceOf[Long])
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Avro logical type ${other} to Catalyst Timestamp type.")
        }

      case (FLOAT, FloatType) =>
        (updater, ordinal, value) =>
          updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (DOUBLE, DoubleType) =>
        (updater, ordinal, value) =>
          updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (STRING, StringType) =>
        (updater, ordinal, value) =>
          val str = value match {
            case s: String => UTF8String.fromString(s)
            case s: Utf8 =>
              val bytes = new Array[Byte](s.getByteLength)
              System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
              UTF8String.fromBytes(bytes)
          }
          updater.set(ordinal, str)

      case (ENUM, StringType) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, UTF8String.fromString(value.toString))

      case (FIXED, BinaryType) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value.asInstanceOf[GenericFixed].bytes().clone())

      case (BYTES, BinaryType) =>
        (updater, ordinal, value) =>
          val bytes = value match {
            case b: ByteBuffer =>
              val bytes = new Array[Byte](b.remaining)
              b.get(bytes)
              bytes
            case b: Array[Byte] => b
            case other => throw new RuntimeException(s"$other is not a valid avro binary.")
          }
          updater.set(ordinal, bytes)

      case (FIXED, d: DecimalType) =>
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromFixed(
            value.asInstanceOf[GenericFixed],
            avroType,
            LogicalTypes.decimal(d.precision, d.scale))
          val decimal = createDecimal(bigDecimal, d.precision, d.scale)
          updater.setDecimal(ordinal, decimal)

      case (BYTES, d: DecimalType) =>
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromBytes(
            value.asInstanceOf[ByteBuffer],
            avroType,
            LogicalTypes.decimal(d.precision, d.scale))
          val decimal = createDecimal(bigDecimal, d.precision, d.scale)
          updater.setDecimal(ordinal, decimal)

      case (RECORD, st: StructType) =>
        val writeRecord = getRecordWriter(avroType, st, path)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[GenericRecord])
          updater.set(ordinal, row)

      case (ARRAY, ArrayType(elementType, containsNull)) =>
        val elementWriter = newWriter(avroType.getElementType, elementType, path)
        (updater, ordinal, value) =>
          val array = value.asInstanceOf[GenericData.Array[Any]]
          val len = array.size()
          val result = createArrayData(elementType, len)
          val elementUpdater = new ArrayDataUpdater(result)

          var i = 0
          while (i < len) {
            val element = array.get(i)
            if (element == null) {
              if (!containsNull) {
                throw new RuntimeException(
                  s"Array value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(elementUpdater, i, element)
            }
            i += 1
          }

          updater.set(ordinal, result)

      case (MAP, MapType(keyType, valueType, valueContainsNull)) if keyType == StringType =>
        val keyWriter = newWriter(SchemaBuilder.builder().stringType(), StringType, path)
        val valueWriter = newWriter(avroType.getValueType, valueType, path)
        (updater, ordinal, value) =>
          val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
          val keyArray = createArrayData(keyType, map.size())
          val keyUpdater = new ArrayDataUpdater(keyArray)
          val valueArray = createArrayData(valueType, map.size())
          val valueUpdater = new ArrayDataUpdater(valueArray)
          val iter = map.entrySet().iterator()
          var i = 0
          while (iter.hasNext) {
            val entry = iter.next()
            assert(entry.getKey != null)
            keyWriter(keyUpdater, i, entry.getKey)
            if (entry.getValue == null) {
              if (!valueContainsNull) {
                throw new RuntimeException(
                  s"Map value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(valueUpdater, i, entry.getValue)
            }
            i += 1
          }

          updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))

      case (UNION, _) =>
        val allTypes = avroType.getTypes.asScala
        val nonNullTypes = allTypes.filter(_.getType != NULL)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            newWriter(nonNullTypes.head, catalystType, path)
          } else {
            nonNullTypes.map(_.getType) match {
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && catalystType == LongType =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case l: java.lang.Long => updater.setLong(ordinal, l)
                    case i: java.lang.Integer => updater.setLong(ordinal, i.longValue())
                  }

              case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && catalystType == DoubleType =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case d: java.lang.Double => updater.setDouble(ordinal, d)
                    case f: java.lang.Float => updater.setDouble(ordinal, f.doubleValue())
                  }

              case _ =>
                catalystType match {
                  case st: StructType if st.length == nonNullTypes.size =>
                    val fieldWriters = nonNullTypes
                      .zip(st.fields)
                      .map {
                        case (schema, field) =>
                          newWriter(schema, field.dataType, path :+ field.name)
                      }
                      .toArray
                    (updater, ordinal, value) =>
                      {
                        val row = new SpecificInternalRow(st)
                        val fieldUpdater = new RowUpdater(row)
                        val i = GenericData.get().resolveUnion(avroType, value)
                        fieldWriters(i)(fieldUpdater, i, value)
                        updater.set(ordinal, row)
                      }

                  case _ =>
                    throw new IncompatibleSchemaException(
                      s"Cannot convert Avro to catalyst because schema at path " +
                        s"${path.mkString(".")} is not compatible " +
                        s"(avroType = $avroType, sqlType = $catalystType).\n")
                }
            }
          }
        } else { (updater, ordinal, value) =>
          updater.setNullAt(ordinal)
        }

      case _ =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Avro to catalyst because schema at path ${path.mkString(".")} " +
            s"is not compatible (avroType = $avroType, sqlType = $catalystType).\n")
    }

  private def getRecordWriter(
      avroType: Schema,
      sqlType: StructType,
      path: List[String]): (RowUpdater, GenericRecord) => Unit = {
    val validFieldIndexes = ArrayBuffer.empty[Int]
    val fieldWriters = ArrayBuffer.empty[(RowUpdater, Any) => Unit]

    val length = sqlType.length
    var i = 0
    while (i < length) {
      val sqlField = sqlType.fields(i)
      val avroField = avroType.getField(sqlField.name)
      if (avroField != null) {
        validFieldIndexes += avroField.pos()

        val baseWriter = newWriter(avroField.schema(), sqlField.dataType, path :+ sqlField.name)
        val ordinal = i
        val fieldWriter = (fieldUpdater: RowUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        fieldWriters += fieldWriter
      } else if (!sqlField.nullable) {
        throw new IncompatibleSchemaException(
          s"""
             |Cannot find non-nullable field ${path
               .mkString(".")}.${sqlField.name} in Avro schema.
             |Source Avro schema: $avroType.
             |Target Catalyst type: $sqlType.
           """.stripMargin)
      }
      i += 1
    }

    (fieldUpdater, record) =>
      {
        var i = 0
        while (i < validFieldIndexes.length) {
          fieldWriters(i)(fieldUpdater, record.get(validFieldIndexes(i)))
          i += 1
        }
      }
  }

  private def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
   * A base interface for updating values inside catalyst data structure like `InternalRow` and
   * `ArrayData`.
   */
  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }
}
