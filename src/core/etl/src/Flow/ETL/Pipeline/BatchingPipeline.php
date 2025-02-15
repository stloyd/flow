<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\chunks_from;
use function Flow\ETL\DSL\from_pipeline;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class BatchingPipeline implements OverridingPipeline, Pipeline
{
    private readonly Pipeline $nextPipeline;

    /**
     * @param Pipeline $pipeline
     * @param int<1, max> $size
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly Pipeline $pipeline, private readonly int $size)
    {
        $this->nextPipeline = $pipeline->cleanCopy();

        /**
         * @psalm-suppress DocblockTypeContradiction
         *
         * @phpstan-ignore-next-line
         */
        if ($this->size <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->size);
        }
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
    }

    public function closure(FlowContext $context) : void
    {
        $this->pipeline->closure($context);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    /**
     * @return array<Pipeline>
     */
    public function pipelines() : array
    {
        $pipelines = [];

        if ($this->pipeline instanceof OverridingPipeline) {
            $pipelines = $this->pipeline->pipelines();
        }

        $pipelines[] = $this->pipeline;

        return $pipelines;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes()->merge($this->nextPipeline->pipes());
    }

    public function process(FlowContext $context) : \Generator
    {
        $this->nextPipeline->setSource(
            chunks_from(
                from_pipeline($this->pipeline),
                $this->size
            )
        );

        return $this->nextPipeline->process($context);
    }

    public function setSource(Extractor $extractor) : self
    {
        $this->pipeline->setSource($extractor);

        return $this;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
