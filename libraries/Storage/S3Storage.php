<?php
namespace packages\s3\Storage;

use packages\base\{IO\Node, IO\Directory, Router, Exception, Storage};
use packages\s3_api\{Configuration, Connector};
use packages\s3\{Directory as S3Directory, Driver as S3Driver};

class S3Storage extends Storage {

	/**
	 * @param array{"@class":class-string<S3Storage>,"root":string,"type":"public"|"protected"|"private","@relative-to"?:string}
	 */
	public static function fromArray(array $data): self {
		foreach (array('type', 'key', 'secret', 'bucket') as $item) {
			if (!isset($data[$item])) {
				throw new Exception("'{$item}' index is not present");
			}
			if (!is_string($data[$item])) {
				throw new Exception("'{$item}' value is not string");
			}
		}
		foreach (array('type', 'key', 'secret', 'bucket', 'endpoint', 'region', 'signature', 'root') as $item) {
			if (isset($data[$item]) and !is_string($data[$item])) {
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

		$driver = new S3Driver($configuration, $data['bucket']);
		$data['root'] = new S3Directory($data['root'] ?? '/');
		$data['root']->setDriver($driver);
		return new self($data['type'], $data['root'], $configuration, $data['bucket']);
	}

	protected string $bucket;
	protected Configuration $configuration;

	public function __construct(string $type, Directory $root, Configuration $configuration, string $bucket) {
		parent::__construct($type, $root);
		if (!$this->root->exists()) {
			$this->root->make(true);
		}
		$this->bucket = $bucket;
		$this->configuration = $configuration;
	}

	public function getURL(Node $node): string {
		if ($this->getType() != self::TYPE_PUBLIC) {
			throw new AccessForbiddenException($node);
		}

		$legacyPathStyle = $this->configuration->getUseLegacyPathStyle();

		$schema = $this->configuration->isSSL() ? 'https://' : 'http://';
		$domain = ($legacyPathStyle ? '' : $this->bucket . '.') . $this->configuration->getEndpoint();
		$path = '/' . ($legacyPathStyle ? $this->bucket . '/' : '') . ltrim($node->getPath(), '/');

		return $schema . $domain . $path;
	}
}
