#!/usr/bin/env php
<?php

declare(strict_types=1);

use Flow\ETL\FlowVersion;
use Flow\ETL\PipelineFactory;
use Flow\ParquetViewer\Command\ReadDataCommand;
use Flow\ParquetViewer\Command\ReadMetadataCommand;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

if ('' !== \Phar::running(false)) {
    require 'phar://flow.phar/vendor/autoload.php';
} else {
    require __DIR__ . '/../vendor/autoload.php';
}

if (false === \in_array(PHP_SAPI, ['cli', 'phpdbg', 'embed'], true)) {
    print PHP_EOL . 'This app may only be invoked from a command line, got "' . PHP_SAPI . '"' . PHP_EOL;

    exit(1);
}

$_ENV['FLOW_PHAR_APP'] = 1;

\ini_set('memory_limit', -1);

$application = new Application('Flow-PHP - Extract Transform Load - Data processing framework', FlowVersion::getVersion());

$application->add((new ReadDataCommand())->setName('parquet:read:data'));
$application->add((new ReadMetadataCommand())->setName('parquet:read:metadata'));
$application->add(new class extends Command {
    public function configure() : void
    {
        $this
            ->setName('run')
            ->setDescription('Run ETL pipeline')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a php file that returns instance of Flow\ETL\DataFrame');
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {
        try {
            /** @phpstan-ignore-next-line */
            $loader = new PipelineFactory((string) $input->getArgument('input-file'));
            $loader->run();
        } catch (\Exception $exception) {
            $style = new SymfonyStyle($input, $output);
            $style->error($exception->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
});

$application->run();