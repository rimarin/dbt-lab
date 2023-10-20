package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.*;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

import java.util.Arrays;
import java.util.Comparator;

/**
 * TablePage header - 32 bytes:
 *  * Bytes 0 - 3 are holding the magic number for index pages.
 *  * Bytes 4 - 7 are holding the page number.
 *  * Bytes 8 - 11 are holding the number of records in the page.
 *  * Bytes 12 - 15 are holding the record width.
 *  * Bytes 16 - 19 are holding the offset of the start of the variable-length chunk.
 *  * Remainder is reserved
 *  *
 */
public class TablePageImpl implements TablePage{

    /**
     * The offset of the field header field containing the page number.
     */
    private static final int HEADER_PAGE_NUMBER_OFFSET = 4;

    /**
     * The offset of the field header field containing the number of records in the page.
     */
    private static final int HEADER_NUMBER_RECORDS_OFFSET = 8;


    /**
     * The offset of the field header field containing the record width.
     */
    private static final int HEADER_RECORD_WIDTH = 12;

    /**
     * The offset of the field header field containing the offset of the start of the variable-length chunk
     */
    private static final int HEADER_OFFSET_VARIABLE_LENGTH_CHUNK = 16;

    /**
     * The offset to the first data field
     */
    private static final int HEADER_TOTAL_BYTES = 32;

    /**
     * The size of metadata in a record
     */
    private static final int RECORD_METADATA_SIZE = 4;

    /**
     * The size of a variable-length field pointer in a record
     */
    private static final int RECORD_POINTER_SIZE = 8;

    /**
     * A flag describing if the contents of the page has been modified since its creation.
     */
    private boolean modified;

    /**
     * A flag for marking whether the table page is expired.
     */
    private boolean expired;

    /**
     * The fixed record width in the table page.
     */
    private int recordWidth;

    /**
     * The schema of the page.
     */
    private TableSchema schema;

    /**
     * The buffer containing the binary page data.
     */
    private byte[] buffer;

    // ------------------------------------------------------------------------
    //                           constructors & set up
    // ------------------------------------------------------------------------

    /**
     * Creates a TablePage that gives access to the tuples stored in binary format
     * in the given byte array. The schema is described in the given TableSchema.
     * The binary array already contains data from some previous TablePage in this case.
     * This class does not really contain a copy of the binary data as objects, but it
     * actually wraps the data and provides functions to access them.
     *
     * @param schema The schema describing the layout of the binary page.
     * @param binaryPage The binary data from the table.
     * @throws PageFormatException If the byte array did not contain a valid page as
     *                             described by the TableSchema.
     */
    public TablePageImpl(TableSchema schema, byte[] binaryPage) throws PageFormatException
    {
        this.schema = schema;
        buffer = binaryPage;
        // Check if the page is valid according to the TableSchema
        if (buffer.length != schema.getPageSize().getNumberOfBytes()) {
            throw new PageFormatException("The page is not valid according to the TableSchema.");
        }
        // Initialize utility variables and flags
        recordWidth = IntField.getIntFromBinary(buffer, HEADER_RECORD_WIDTH);
        modified = false;
        expired = false;
    }

    /**
     * Initializes an empty table page with the given page number
     * that will store its data in the given byte buffer.
     * This method makes sure a valid header is created.
     *
     * @param schema The schema describing the layout of the new page.
     * @param binaryPage The buffer for the table page.
     * @param newPageNumber The number for the initialized page.
     * @throws PageFormatException If the array size does not match the page size
     *                             for this table schema.
     */
    public TablePageImpl(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException
    {
        if (schema.getPageSize().getNumberOfBytes() != binaryPage.length) {
            throw new PageFormatException("The given byte array does not match the page size for the given schema.");
        }

        this.schema = schema;
        buffer = binaryPage;

        // Initialize utility variables and flags and page number if needed
        modified = true;
        expired = false;

        // Generate a valid header

        // Insert the magic number into the class
        new IntField(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER).encodeBinary(buffer, 0);

        // Insert new page number into header
        new IntField(newPageNumber).encodeBinary(buffer, HEADER_PAGE_NUMBER_OFFSET);

        // Insert number of records into header, initially 0
        new IntField(0).encodeBinary(buffer, HEADER_NUMBER_RECORDS_OFFSET);

        // Compute and save record width
        // Record consists of [ Metadata | Fixed-Length Values | Variable-Length Pointers ]
        // Metadata is a 4-bytes IntField
        int fixedLengthValuesSize = 0;
        int numVariableLengthPointers = 0;
        for (int i = 0; i < this.schema.getNumberOfColumns(); i++) {
            if (this.schema.getColumn(i).getDataType().isFixLength()) {
                fixedLengthValuesSize += this.schema.getColumn(i).getDataType().getNumberOfBytes();
            }
            else{
                numVariableLengthPointers += 1;
            }
        }
        int variableLengthPointersSize = new BigIntField(0).getNumberOfBytes() * numVariableLengthPointers;
        recordWidth = RECORD_METADATA_SIZE + fixedLengthValuesSize + variableLengthPointersSize;
        new IntField(recordWidth).encodeBinary(buffer, HEADER_RECORD_WIDTH);

        // Save offset of variable-length chunk, initialized to the end of the page
        IntField offsetVariableLengthChunk = new IntField(this.schema.getPageSize().getNumberOfBytes());
        offsetVariableLengthChunk.encodeBinary(buffer, HEADER_OFFSET_VARIABLE_LENGTH_CHUNK);
    }

    /**
     * This method checks, if the object has been modified since it was read from
     * the persistent storage. Objects that have been changed must be written back
     * before they can be evicted from the cache.
     *
     * @return true, if the data object has been modified, false if it is unchanged.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public boolean hasBeenModified() throws PageExpiredException {
        if (isExpired()) throw new PageExpiredException();
        return modified;
    }

    /**
     * Marks this cached data object as invalid. This method should be called, when the
     * contents of the buffer behind the cached object is no longer in the cache, such that
     * this cacheable data object would now work on invalid data.
     */
    @Override
    public void markExpired() {
        expired = true;
    }

    /**
     * Checks if this cacheable object is actually expired. An object is expired, when the
     * contents in the buffer behind that object is no longer in the cache or has been
     * overwritten.
     *
     * @return True, if the object is expired (no longer valid), false otherwise.
     */
    @Override
    public boolean isExpired() {
        return expired;
    }

    /**
     * Gets the binary buffer that contains the data that is wrapped by this page. All operations on this
     * page will affect the buffer that is returned by this method.
     *
     * @return The buffer containing the data wrapped by this page object.
     */
    @Override
    public byte[] getBuffer() {
        return buffer;
    }

    /**
     * Gets the page number of this page, as is found in the header bytes 4 - 7.
     *
     * @return The page number from the page header.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public int getPageNumber() throws PageExpiredException {
        if (isExpired()) throw new PageExpiredException();
        return IntField.getIntFromBinary(buffer, HEADER_PAGE_NUMBER_OFFSET);
    }

    /**
     * Gets how many records are currently stored on this page. This returns the total number
     * of records, including those that are marked as deleted. The number retrieved by this
     * function is the value from the header bytes 8 - 11.
     *
     * @return The total number of records on this page.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public int getNumRecordsOnPage() throws PageExpiredException {
        if (isExpired()) throw new PageExpiredException();
        return IntField.getIntFromBinary(buffer, HEADER_NUMBER_RECORDS_OFFSET);
    }

    /**
     * Inserts a tuple into the page by inserting the variable-length fields into the dedicated
     * part of the page and inserting the record for the tuple into the record sequence.
     * <p>
     * If the method is not successful in inserting the tuple due to the fact that there is
     * not enough space left, it returns false, but does not throw an exception.
     *
     * @param tuple The tuple to be inserted.
     * @return true, if the tuple was inserted, false, if the tuple was not inserted.
     * @throws PageFormatException Thrown, if the format of the page is invalid, such as that
     *                             current offset to the variable-length-chunk is invalid.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public boolean insertTuple(DataTuple tuple) throws PageFormatException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        // 1. Check if the tuple fits into the page

        // Get number of bytes needed to insert tuple, different from record width as we are considering variable length
        int tupleSize = RECORD_METADATA_SIZE;
        for (int i = 0; i < tuple.getNumberOfFields(); i++) {

            if (tuple.getField(i).isNULL()){
                if (tuple.getField(i).getBasicType().isFixLength()){
                    tupleSize += schema.getColumn(i).getDataType().getNumberOfBytes();
                }
                else {
                    tupleSize += RECORD_POINTER_SIZE;
                }
            }
            else {
                tupleSize += tuple.getField(i).getNumberOfBytes();
                if (!tuple.getField(i).getBasicType().isFixLength()){
                    tupleSize += RECORD_POINTER_SIZE;
                }
            }
        }

        // Compute the number of available bytes in the page
        int availableBytes = schema.getPageSize().getNumberOfBytes()- HEADER_TOTAL_BYTES;
        availableBytes -= recordWidth * getNumRecordsOnPage();
        availableBytes -= (schema.getPageSize().getNumberOfBytes() - IntField.getIntFromBinary(buffer, HEADER_OFFSET_VARIABLE_LENGTH_CHUNK));
        if (availableBytes < tupleSize){
            return false;
        }

        // 2. Check if the format of the page is valid.
        if (schema.getNumberOfColumns() != tuple.getNumberOfFields()) {
            throw new PageFormatException("Different amount of fields in tuple than expected");
        }
        for (int i = 0; i < tuple.getNumberOfFields(); i++) {
            if (!schema.getColumn(i).getDataType().getBasicType().equals(tuple.getField(i).getBasicType())){
                throw new PageFormatException("A field has a different basic type than expected");
            }
        }

        // 3. Encode the fields of the tuple into the buffer
        // Compute the starting position: offset of the record should be record width * number of records + header
        int offset = recordWidth * getNumRecordsOnPage() + HEADER_TOTAL_BYTES;
        // Insert the metadata
        IntField metadata = new IntField(0);
        metadata.encodeBinary(buffer, offset);
        offset += RECORD_METADATA_SIZE;
        // Insert the fixed-length values
        for (int i = 0; i < tuple.getNumberOfFields(); i++) {
            DataField dataField = tuple.getField(i);
            if (dataField.getBasicType().isFixLength()) {
                dataField.encodeBinary(buffer, offset);
                // null values in fixed size variable would otherwise take less space than the normal amount
                offset += schema.getColumn(i).getDataType().getNumberOfBytes();
            }
            else {
                // handle null values
                if (dataField.isNULL()) {
                    BigIntField pointer = new BigIntField(0);
                    pointer.encodeBinary(buffer, offset);
                    offset += RECORD_POINTER_SIZE;
                }
                else {
                    // Point where variable chunk will be stored
                    IntField offsetVariableLengthChunk = new IntField(IntField.getIntFromBinary(buffer, HEADER_OFFSET_VARIABLE_LENGTH_CHUNK) - dataField.getNumberOfBytes());
                    // Change offset to variable length chunk
                    offsetVariableLengthChunk.encodeBinary(buffer, HEADER_OFFSET_VARIABLE_LENGTH_CHUNK);

                    // Insert the variable-length values. Variable-Length-Field-Pointers are stored as BigIntField (8 bytes).
                    // Lower 4 byte are offset to field on the page.
                    // Higher 4 byte are length of the field.
                    BigIntField fieldPointer = new BigIntField((long)offsetVariableLengthChunk.getValue() |
                            ((long)dataField.getNumberOfBytes() << 32));
                    fieldPointer.encodeBinary(buffer, offset);

                    // Encode data
                    dataField.encodeBinary(buffer, offsetVariableLengthChunk.getValue());

                    // Update offset to insert next field of tuple
                    offset += RECORD_POINTER_SIZE;
                }
            }
        }

        // 4. Update number of records on page
        IntField numRecordsOnPage = new IntField(getNumRecordsOnPage() + 1);
        numRecordsOnPage.encodeBinary(buffer, HEADER_NUMBER_RECORDS_OFFSET);

        modified = true;
        return true;
    }

    /**
     * Deletes a tuple by setting the tombstone flag to 1.
     *
     * @param position The position of the tuple's record. The first record has position 0.
     * @throws PageTupleAccessException Thrown, if the index is negative or larger than the number
     *                                  of tuple on the page.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public void deleteTuple(int position) throws PageTupleAccessException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        if (position < 0 || position >= getNumRecordsOnPage()){
            throw new PageTupleAccessException(position);
        }

        int positionInBuffer = HEADER_TOTAL_BYTES + recordWidth * position;
        IntField tupleMetaData = IntField.getFieldFromBinary(buffer, positionInBuffer);

        // & 1 is a mask that takes only the lsb of the integer
        if ((tupleMetaData.getValue() & 1) == 1){
            return;
            //throw new PageTupleAccessException(position);
        }

        // Set the tombstone flag to 1, since it is the lsb, it is done by adding 1
        tupleMetaData.add(1);
        tupleMetaData.encodeBinary(buffer, positionInBuffer);

        modified = true;
    }

    /**
     * Takes the DataTuple from the page whose record is found at the given position in the
     * sequence of records on the page.
     * <p>
     * The position starts at 0, such that
     * <code>getDataTuple(0, ...)</code> returns the tuple whose record starts directly after
     * the page header.
     * <p>
     * The schema of the returned tuple is defined by the parameters <code>columnBitmap</code> and
     * <code>numCols</code>. Only the columns for which the respective bit in <code>columnBitmap</code> is set are
     * returned.
     * <p>
     * For variable-lengths fields, the method takes care of resolving the pointers in the fixed record and returns
     * the contents of the field.
     * <p>
     * If the tombstone flag of the record is set, then this method returns null, but does
     * not throw an exception.
     *
     * @param position The position of the tuple's record. The first record has position 0.
     * @param columnBitmap The bitmap describing which columns to fetch. See description of the class
     *                     for details on how the bitmaps describe which columns to fetch.
     *                     A columnBitmap of 0110101 (in binary representation) will fetch the first,
     *                     the third, the fifth and the sixth column.
     *                     Amount of ones in the binary representation of columnBitmap should probably
     *                     correspond to numCols
     * @param numCols The number of columns that should be fetched.
     *
     * @return The tuple constructed from the record and its referenced variable-length fields,
     *         or null, if the tombstone bit of the tuple is set.
     *
     * @throws PageTupleAccessException Thrown, if the tuple could not be constructed (pointers invalid),
     *                                  or the index negative or larger than the number of tuple on the page.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    @Override
    public DataTuple getDataTuple(int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException {
        return getDataTuple(null, position, columnBitmap, numCols);
    }


    @Override
    public DataTuple getDataTuple(LowLevelPredicate[] preds, int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        if (position < 0 || position >= getNumRecordsOnPage()) throw new PageTupleAccessException(position);
        if (numCols > schema.getNumberOfColumns()) throw new PageTupleAccessException(numCols);
        if (Long.bitCount(columnBitmap) < numCols) throw new PageTupleAccessException(Long.bitCount(columnBitmap));

        int positionInBuffer = HEADER_TOTAL_BYTES + recordWidth * position;

        IntField tupleMetaData = IntField.getFieldFromBinary(buffer, positionInBuffer);
        // & 1 is a mask that takes only the lsb of the integer
        if ((tupleMetaData.getValue() & 1) == 1){
            return null;
        }

        //metadata not needed anymore
        positionInBuffer += RECORD_METADATA_SIZE;

        //if the evaluated predicates are wrong for the tuple at positionInBuffer -> return null
        if (!evaluatePredicates(preds, positionInBuffer)) return null;


        DataTuple dataTuple = new DataTuple(numCols);
        int offsetInRecord = 0;
        int currentColumn = 0;
        for (int i = 0; (i < numCols) && (columnBitmap != 0); columnBitmap >>>= 1) {
            // 0 in the bitmap = column not needed
            if ((columnBitmap & 0x1) == 0) {
                if (schema.getColumn(currentColumn).getDataType().isFixLength()) {
                    offsetInRecord += schema.getColumn(currentColumn).getDataType().getNumberOfBytes();
                }
                else {
                    offsetInRecord += RECORD_POINTER_SIZE;
                }
                currentColumn++;
            }
            // 1 in the bitmap = column to be fetched
            else {
                // If fixed length read data directly from field
                if (schema.getColumn(currentColumn).getDataType().isFixLength()){
                    DataField dataField = schema.getColumn(currentColumn).getDataType().
                            getFromBinary(buffer,
                                    positionInBuffer + offsetInRecord,
                                    schema.getColumn(currentColumn).getDataType().getNumberOfBytes());
                    dataTuple.assignDataField(dataField, i);
                    offsetInRecord += schema.getColumn(currentColumn).getDataType().getNumberOfBytes();

                }
                // If variable length data read from position where the pointer of this field is pointing to
                else {
                    BigIntField pointer = BigIntField.getFieldFromBinary(buffer, positionInBuffer + offsetInRecord);
                    // Shift the pointer to either only get the 32msb or the 32lsb to get the length and offset of the data
                    long lengthPointer = pointer.getValue() >> 32;
                    long offsetPointer = pointer.getValue() << 32;
                    offsetPointer = offsetPointer >> 32;

                    DataField dataField;
                    // Handle null value
                    if (offsetPointer == 0){
                        dataField = schema.getColumn(currentColumn).getDataType().getNullValue();
                    } else {
                        dataField = schema.getColumn(currentColumn).getDataType().getFromBinary(buffer, (int)offsetPointer, (int)lengthPointer);
                    }
                    // ((VarcharField) dataField).getValue()
                    dataTuple.assignDataField(dataField, i);
                    offsetInRecord += RECORD_POINTER_SIZE;
                }
                currentColumn++;
                i++;
            }
        }

        return dataTuple;
    }


    private boolean evaluatePredicates(LowLevelPredicate[] preds, int positionInBuffer){
        //todo are predicates with the same columnIndex handled
        if (preds == null) return true;

        int numCols = preds.length;
        long columnBitmap = 0;

        Arrays.sort(preds, Comparator.comparingInt(LowLevelPredicate::getColumnIndex));

        for (LowLevelPredicate pred : preds){
            long helper = 1;
            columnBitmap |= helper << pred.getColumnIndex();
        }

        int offsetInRecord = 0;
        int currentColumn = 0;
        for (int i = 0; (i < numCols) && (columnBitmap != 0); columnBitmap >>>= 1) {
            // 0 in the bitmap = column not needed
            if ((columnBitmap & 0x1) == 0) {
                if (schema.getColumn(currentColumn).getDataType().isFixLength()) {
                    offsetInRecord += schema.getColumn(currentColumn).getDataType().getNumberOfBytes();
                } else {
                    offsetInRecord += RECORD_POINTER_SIZE;
                }
                currentColumn++;
            } // 1 in the bitmap = evaluate the field with the corresponding predicate
            else {
                // If fixed length read data directly from field
                if (schema.getColumn(currentColumn).getDataType().isFixLength()){
                    DataField dataField = schema.getColumn(currentColumn).getDataType().
                            getFromBinary(buffer, positionInBuffer + offsetInRecord,
                                    schema.getColumn(currentColumn).getDataType().getNumberOfBytes());
                    offsetInRecord += schema.getColumn(currentColumn).getDataType().getNumberOfBytes();

                    // evaluate predicate. Since the predicates are sorted after their columnIndex we can directly access them
                    for (LowLevelPredicate pred : preds){
                        if (pred.getColumnIndex() == currentColumn){
                            if (!pred.evaluateWithNull(dataField)) return false;
                        }
                    }


                }
                // If variable length data read from position where the pointer of this field is pointing to
                else {
                    BigIntField pointer = BigIntField.getFieldFromBinary(buffer, positionInBuffer + offsetInRecord);
                    // Shift the pointer to either only get the 32msb or the 32lsb to get the length and offset of the data
                    long lengthPointer = pointer.getValue() >> 32;
                    long offsetPointer = pointer.getValue() << 32;
                    offsetPointer = offsetPointer >> 32;

                    DataField dataField;
                    // Handle null value
                    if (offsetPointer == 0){
                        dataField = schema.getColumn(currentColumn).getDataType().getNullValue();
                    } else {
                        dataField = schema.getColumn(currentColumn).getDataType().getFromBinary(buffer, (int)offsetPointer, (int)lengthPointer);
                    }
                    offsetInRecord += RECORD_POINTER_SIZE;

                    // evaluate predicate. Since the predicates are sorted after their columnIndex we can directly access them
                    for (LowLevelPredicate pred : preds){
                        if (pred.getColumnIndex() == currentColumn){
                            if (!pred.evaluateWithNull(dataField)) return false;
                        }
                    }
                }
                currentColumn++;
                i++;
            }
        }

        // the evaluation of every predicate succeeded
        return true;
    }

    @Override
    public TupleIterator getIterator(int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        if (numCols > schema.getNumberOfColumns()) throw new PageTupleAccessException(numCols);
        if (Long.bitCount(columnBitmap) < numCols) throw new PageTupleAccessException(Long.bitCount(columnBitmap));

        return new TupleIteratorImpl(null, numCols, columnBitmap, this);
    }

    @Override
    public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        if (numCols > schema.getNumberOfColumns()) throw new PageTupleAccessException(numCols);
        if (Long.bitCount(columnBitmap) < numCols) throw new PageTupleAccessException(Long.bitCount(columnBitmap));

        return new TupleIteratorImpl(preds, numCols, columnBitmap, this);
    }

    @Override
    public TupleRIDIterator getIteratorWithRID() throws PageTupleAccessException, PageExpiredException {
        if (isExpired()) throw new PageExpiredException();

        return new TupleRIDIteratorImpl(this);
    }

    public TableSchema getSchema(){
        return schema;
    }
}
