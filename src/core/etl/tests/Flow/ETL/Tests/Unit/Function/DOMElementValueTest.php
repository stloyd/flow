<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, row};
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Tests\FlowTestCase;

final class DOMElementValueTest extends FlowTestCase
{
    public function test_getting_element_value_with_children() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo><bar>baz</bar></foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            'baz',
            ref('value')->domElementValue()->eval(row((new EntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }

    public function test_getting_simple_element_value() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            'bar',
            ref('value')->domElementValue()->eval(row((new EntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }
}
