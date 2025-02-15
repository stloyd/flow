<?php declare(strict_types=1);
namespace Flow\Parquet\Thrift;

/**
 * Autogenerated by Thrift Compiler (0.19.0).
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;

class RowGroup extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'columns',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => [
                'type' => TType::STRUCT,
                'class' => '\Flow\Parquet\Thrift\ColumnChunk',
            ],
        ],
        2 => [
            'var' => 'total_byte_size',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        3 => [
            'var' => 'num_rows',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        4 => [
            'var' => 'sorting_columns',
            'isRequired' => false,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => [
                'type' => TType::STRUCT,
                'class' => '\Flow\Parquet\Thrift\SortingColumn',
            ],
        ],
        5 => [
            'var' => 'file_offset',
            'isRequired' => false,
            'type' => TType::I64,
        ],
        6 => [
            'var' => 'total_compressed_size',
            'isRequired' => false,
            'type' => TType::I64,
        ],
        7 => [
            'var' => 'ordinal',
            'isRequired' => false,
            'type' => TType::I16,
        ],
    ];

    public static $isValidate = false;

    /**
     * Metadata for each column chunk in this row group.
     * This list must have the same order as the SchemaElement list in FileMetaData.
     *
     * @var \Flow\Parquet\Thrift\ColumnChunk[]
     */
    public $columns;

    /**
     * Byte offset from beginning of file to first page (data or dictionary)
     * in this row group *.
     *
     * @var int
     */
    public $file_offset;

    /**
     * Number of rows in this row group *.
     *
     * @var int
     */
    public $num_rows;

    /**
     * Row group ordinal in the file *.
     *
     * @var int
     */
    public $ordinal;

    /**
     * If set, specifies a sort ordering of the rows in this RowGroup.
     * The sorting columns can be a subset of all the columns.
     *
     * @var \Flow\Parquet\Thrift\SortingColumn[]
     */
    public $sorting_columns;

    /**
     * Total byte size of all the uncompressed column data in this row group *.
     *
     * @var int
     */
    public $total_byte_size;

    /**
     * Total byte size of all compressed (and potentially encrypted) column data
     * in this row group *.
     *
     * @var int
     */
    public $total_compressed_size;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'RowGroup';
    }

    public function read($input)
    {
        return $this->_read('RowGroup', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('RowGroup', self::$_TSPEC, $output);
    }
}
