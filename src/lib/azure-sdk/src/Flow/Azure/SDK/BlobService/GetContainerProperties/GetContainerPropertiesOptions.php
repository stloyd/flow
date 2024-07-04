<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetContainerProperties;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class GetContainerPropertiesOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?string $leaseId = null;

    private ?string $requestId = null;

    private ?int $timeoutSeconds = null;

    private ?string $version = BlobService::VERSION;

    private ?string $versionId = null;

    public function toHeaders() : array
    {
        $headers = [];

        $headers['user-agent'] = $this->userAgentHeader();

        if ($this->version !== null) {
            $headers['x-ms-version'] = $this->version;
        }

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->leaseId !== null) {
            $headers['x-ms-lease-id'] = $this->leaseId;
        }

        return $headers;
    }

    public function toURIParameters() : array
    {
        $uriParameters = [];

        if ($this->versionId !== null) {
            $uriParameters['versionId'] = $this->versionId;
        }

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = $this->timeoutSeconds;
        }

        return $uriParameters;
    }

    public function withLeaseId(string $leaseId) : self
    {
        $this->leaseId = $leaseId;

        return $this;
    }

    public function withRequestId(string $requestId) : self
    {
        $this->requestId = $requestId;

        return $this;
    }

    public function withTimeoutSeconds(int $timeoutSeconds) : self
    {
        $this->timeoutSeconds = $timeoutSeconds;

        return $this;
    }

    public function withVersion(string $version) : self
    {
        $this->version = $version;

        return $this;
    }

    public function withVersionId(string $versionId) : self
    {
        $this->versionId = $versionId;

        return $this;
    }
}