<?php declare(strict_types=1);

namespace Flow\RDSL;

use Flow\RDSL\AccessControl\DenyAll;
use Flow\RDSL\Exception\InvalidArgumentException;

final class Finder
{
    /**
     * @var array<DSLNamespace>
     */
    private readonly array $namespaces;

    /**
     * @param array<DSLNamespace> $namespaces - list of namespaces to search for DSL functions
     */
    public function __construct(
        array $namespaces,
        private readonly AccessControl $entryPointACL
    ) {
        $namespaces = \array_map(static fn (DSLNamespace $n) : DSLNamespace => $n, $namespaces);

        $globalNamespace = null;

        foreach ($namespaces as $namespace) {
            if ($namespace->name === '\\') {
                $globalNamespace = $namespace;
            }
        }

        if ($globalNamespace === null) {
            $namespaces[] = new DSLNamespace('\\', new DenyAll());
        }

        $this->namespaces = $namespaces;
    }

    public function findClass(string $name) : \ReflectionClass
    {
        if (\class_exists($name)) {
            return new \ReflectionClass($name);
        }

        $simpleName = \ltrim($name, '\\');

        foreach ($this->namespaces as $namespace) {
            $function = $namespace->name . '\\' . $simpleName;

            if (\class_exists($function)) {
                return new \ReflectionClass($function);
            }
        }

        if (\count($this->namespaces)) {
            throw new InvalidArgumentException(\sprintf('Class "%s" does not exists in namespaces: "%s"', $name, \implode('", "', \array_map(static fn (DSLNamespace $n) => $n->name, $this->namespaces))));
        }

        throw new InvalidArgumentException(\sprintf('Class "%s" does not exists', $name));
    }

    public function findFunction(string $name, bool $entryPoint) : \ReflectionFunction
    {
        if (\function_exists($name)) {
            $reflection = new \ReflectionFunction($name);

            if ($reflection->getNamespaceName() === '') {
                foreach ($this->namespaces as $namespace) {
                    if ($namespace->isEqual('\\')) {
                        if ($namespace->isAllowed($name)) {
                            if ($entryPoint && $this->entryPointACL->isAllowed($name) === false) {
                                throw new InvalidArgumentException(\sprintf('Function "%s" from namespace "%s" is not allowed to be executed as an entry point.', $name, $reflection->getNamespaceName()));
                            }

                            return $reflection;
                        }
                    }
                }

                throw new InvalidArgumentException(\sprintf('Function "%s" from global namespace is not allowed to be executed.', $name));
            }

            foreach ($this->namespaces as $namespace) {
                if ('\\' . $namespace->name === $reflection->getNamespaceName()) {
                    if ($namespace->isAllowed($name)) {
                        if ($entryPoint && $this->entryPointACL->isAllowed($name) === false) {
                            throw new InvalidArgumentException(\sprintf('Function "%s" from namespace "%s" is not allowed to be executed as an entry point.', $name, $reflection->getNamespaceName()));
                        }

                        return $reflection;
                    }
                }
            }

            throw new InvalidArgumentException(\sprintf('Function "%s" from namespace "%s" is not allowed to be executed.', $name, $reflection->getNamespaceName()));
        }

        $simpleName = \ltrim($name, '\\');

        foreach ($this->namespaces as $namespace) {
            $function = $namespace->name . '\\' . $simpleName;

            if (\function_exists($function)) {
                if ($namespace->isAllowed($simpleName) === false) {
                    throw new InvalidArgumentException(\sprintf('Function "%s" from namespace "%s" is not allowed to be executed.', $simpleName, $namespace->name));
                }

                if ($entryPoint && $this->entryPointACL->isAllowed($name) === false) {
                    throw new InvalidArgumentException(\sprintf('Function "%s" from namespace "%s" is not allowed to be executed as an entry point.', $name, $namespace->name));
                }

                return new \ReflectionFunction($function);
            }
        }

        if (\count($this->namespaces)) {
            throw new InvalidArgumentException(\sprintf('Function "%s" does not exists in namespaces: "%s"', $name, \implode('", "', \array_map(static fn ($n) => $n->name, $this->namespaces))));
        }

        throw new InvalidArgumentException(\sprintf('Function "%s" does not exists', $name));
    }
}