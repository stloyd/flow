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

/**
 * The hash function used in Bloom filter. This function takes the hash of a column value
 * using plain encoding.
 */
class BloomFilterHash extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'XXHASH',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\Flow\Parquet\Thrift\XxHash',
        ],
    ];

    public static $isValidate = false;

    /**
     * xxHash Strategy. *.
     *
     * @var \Flow\Parquet\Thrift\XxHash
     */
    public $XXHASH;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'BloomFilterHash';
    }

    public function read($input)
    {
        return $this->_read('BloomFilterHash', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('BloomFilterHash', self::$_TSPEC, $output);
    }
}
