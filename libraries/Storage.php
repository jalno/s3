<?php

namespace packages\s3;

use packages\base\Exception;
use packages\base\IO\Directory as DirectoryAbstract;
use packages\base\IO\Node;
use packages\base\Storage\AccessForbiddenException;
use packages\base\Storage as StorageAbstract;
use packages\s3_api\Configuration;

class Storage extends StorageAbstract
{
    /**
     * @param array{"@class":class-string<Storage>, "type":"public"|"protected"|"private", "key":string, "secret":string, "bucket":string, "root"?:string, "endpoint"?:string, "region"?:string, "signature"?:"v2"|"v4"} $data
     */
    public static function fromArray(array $data): self
    {
        foreach (['type', 'key', 'secret', 'bucket'] as $item) {
            if (!isset($data[$item])) {
                throw new Exception("'{$item}' index is not present");
            }
            if (!is_string($data[$item])) {
                throw new Exception("'{$item}' value is not string");
            }
        }
        foreach (['type', 'key', 'secret', 'bucket', 'endpoint', 'region', 'signature', 'root'] as $item) {
            if (!isset($data[$item])) {
                continue;
            }
            if (!is_string($data[$item])) {
                throw new Exception("'{$item}' value is not string");
            }
        }

        $configuration = new Configuration(
            $data['key'], $data['secret'], ($data['signature'] ?? 'v2'), ($region['region'] ?? null), ($data['endpoint'] ?? null)
        );
        if (isset($data['use_ssl'])) {
            $configuration->setSSL(in_array($data['use_ssl'], [1, true, '1', 'on', 'yes', 'true']));
        }
        if (isset($data['legacy_style_path'])) {
            $configuration->setUseLegacyPathStyle(in_array($data['legacy_style_path'], [1, true, '1', 'on', 'yes', 'true']));
        }
        /** @var "private"|"protected"|"public" */
        $type = $data['type'];
        /** @var string */
        $bucket = $data['bucket'];

        $driver = new Driver($configuration, $bucket);
        $data['root'] = new Directory($data['root'] ?? '/');
        $data['root']->setDriver($driver);

        return new self($type, $data['root'], $configuration, $bucket);
    }

    protected string $bucket;
    protected Configuration $configuration;

    public function __construct(string $type, DirectoryAbstract $root, Configuration $configuration, string $bucket)
    {
        parent::__construct($type, $root);
        $this->bucket = $bucket;
        $this->configuration = $configuration;
    }

    public function getURL(Node $node): string
    {
        if (self::TYPE_PUBLIC != $this->getType()) {
            throw new AccessForbiddenException($node);
        }

        $legacyPathStyle = $this->configuration->getUseLegacyPathStyle();

        $schema = $this->configuration->isSSL() ? 'https://' : 'http://';
        $domain = ($legacyPathStyle ? '' : $this->bucket.'.').$this->configuration->getEndpoint();
        $path = '/'.($legacyPathStyle ? $this->bucket.'/' : '').ltrim($node->getPath(), '/');

        return $schema.$domain.$path;
    }
}
