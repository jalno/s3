<?php
namespace packages\s3;

use packages\s3_api\{Acl, Connector, Configuration, Input};
use packages\s3_api\Exception\{CannotDeleteFile, CannotGetBucket, CannotGetFile, CannotPutFile};
use packages\base\{IO, IO\File, Options};

class Driver {

	/**
	 * @return null|array{"configuration": Configuration, "bucket": string}
	 */
	public static function getConfigurationByName(?string $name = 'default'): ?array {
		$option = Options::get('packages.s3.configuration.' . $name);
		if (!$option and (!$name or $name == 'default')) {
			$option = Options::get('packages.s3.configuration');
		}
		if (!$option or !is_array($option) or
			(!isset($option['key']) and
			!isset($option['access'])) or
			!isset($option['secret']) or
			!isset($option['bucket'])
		) {
			return null;
		}

		$configuration = new Configuration(
			($option['key'] ?? $option['access']),
			$option['secret']
		);

		if (isset($option['signature']) and $option['signature']) {
			$configuration->setSignatureMethod($option['signature']);
		}
		if (isset($option['region']) and $option['region']) {
			$configuration->setRegion($option['region']);
		}
		if (isset($option['endpoint']) and $option['endpoint']) {
			$configuration->setEndpoint($option['endpoint']);
		}
		if (isset($option['use_ssl'])) {
			$configuration->setSSL(boolval($option['use_ssl']));
		}
		if (isset($option['legacy_style_path'])) {
			$configuration->setUseLegacyPathStyle(boolval($option['legacy_style_path']));
		}
		return array(
			'configuration' => $configuration,
			'bucket' => $option['bucket'],
		);
	}

	protected string $bucket;
	protected Connector $connector;
	protected Configuration $configuration;

	public function __construct(Configuration $configuration, string $bucket) {
		$this->bucket = $bucket;
		$this->configuration = $configuration;
		$this->connector = new Connector($this->configuration);
	}

	public function getBucket(): string {
		return $this->bucket;
	}

	public function getConnector(): Connector {
		return $this->connector;
	}

	public function getConfiguration(): Configuration {
		return $this->configuration;
	}

	/**
	 * @param array<string|int, mixed> $requestHeaders
	 */
	public function upload(File\Local $local, string $remote, string $acl = Acl::ACL_PUBLIC_READ, array $requestHeaders = []): void {
		$remote = $this->normalizePath($remote);
		try {
			$this->getConnector()->putObject(
				Input::createFromFile($local),
				$this->bucket,
				$remote,
				$acl,
				$requestHeaders
			);
		} catch (CannotPutFile $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}

	public function download(string $remote, File\Local $local): bool {
		$remote = $this->normalizePath($remote);
		try {
			$this->getConnector()->getObject(
				$this->bucket,
				$remote,
				$local,
			);
			return true;
		} catch (CannotGetFile $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}
	/**
	 * @param array<string|int, mixed> $requestHeaders
	 */
	public function put_contents(string $remote, string $data, string $acl = Acl::ACL_PUBLIC_READ, array $requestHeaders = []): bool {
		$remote = $this->normalizePath($remote);
		try {
			$this->getConnector()->putObject(
				Input::createFromData($data),
				$this->bucket,
				$remote,
				$acl,
				$requestHeaders
			);
			return true;
		} catch (CannotPutFile $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}
	public function get_contents(string $remote, ?int $from = null, ?int $to = null): string {
		$remote = $this->normalizePath($remote);
		try {
			return $this->getConnector()->getObject(
				$this->bucket,
				$remote,
				null,
				$from,
				$to
			) ?? '';
		} catch (CannotGetFile $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}
	public function mkdir(string $remote): bool {
		$input = Input::createForDirectory();
		try {
			$this->getConnector()->putObject(
				$input,
				$this->bucket,
				rtrim($this->normalizePath($remote), '/') . '/',
				Acl::ACL_PUBLIC_READ_WRITE
			);
			return true;
		} catch (CannotPutFile $e) {
			return false;
		}
	}
	public function unlink(string $remote): void {
		$remote = $this->normalizePath($remote);
		try {
			$this->getConnector()->deleteObject(
				$this->bucket,
				$remote,
			);
		} catch (CannotDeleteFile $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}

	public function size(string $remote): int {
		try {
			$result = $this->getConnector()->headObject(
				$this->bucket,
				$this->normalizePath($remote),
			);
			return (int) $result['size'] ?? 0;
		} catch (CannotGetFile $e) {
			return 0;
		}
	}
	public function exists(string $remote): bool {
		try {
			$result = $this->getConnector()->headObject(
				$this->bucket,
				$this->normalizePath($remote),
			);
			if (isset($result['size'])) {
				return true;
			}
		} catch (CannotGetFile $e) {}
		return false;
	}
	public function directoryDelete(string $remote): bool {
		try {
			$this->getConnector()->deleteObject(
				$this->bucket,
				rtrim($this->normalizePath($remote), '/') . '/',
			);
			return true;
		} catch (CannotDeleteFile $e) {
			return false;
		}
	}
	public function directorySize(string $remote): int {
		return array_reduce(
			$this->directoryFiles($remote, true),
			fn(int $carry, array $file) => $carry + intval($file['size'] ?? 0),
			0
		);
	}
	public function directoryExists(string $remote): bool {
		$remote = rtrim($this->normalizePath($remote), '/') . '/';

		$result = $this->getConnector()->getBucket($this->bucket, $remote, null, null, '/', true);
		$lenght = strlen($remote);
		foreach ($result as $key => $val) {
			if (substr($val['name'] ?? $val['prefix'], 0, $lenght) == $remote) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @return string[]
	 */
	public function directoryDirectories(string $remote, bool $recursively = false): array {
		$result = [];
		$remote = rtrim($this->normalizePath($remote), '/') . '/';
		$items = $this->directoryItems($remote, true);
		foreach ($items as $item) {
			$parts = explode('/', ($item['name'] ?? $item['prefix']));
			$count = count($parts) - 1; // don't care about last part, it may be empty or file name

			$path = '';
			for ($x = 0; $x < $count; $x++) {
				if (!$recursively and $x > 1) {
					break;
				}
				$path .= $parts[$x] . '/';
				if ($path == $remote) {
					continue;
				}
				if (!in_array($path, $result)) {
					$result[] = $path;
				}
			}
		}
		return $result;
	}

	/**
	 * @return array<string, array{"name": string, "prefix": string}>
	 */
	public function directoryFiles(string $remote, bool $recursively = false): array {
		return array_filter(
			$this->directoryItems($remote, $recursively),
			fn (array $item) => (isset($item['name']) and substr($item['name'], -1) !== '/')
		);
	}

	/**
	 * @return array<string, array{"name": string, "prefix": string}>
	 */
	public function directoryItems(string $remote, bool $recursively = false): array {
		$remote = rtrim($this->normalizePath($remote), '/') . '/';
		try {
			return $this->getConnector()->getBucket($this->bucket, $remote, null, null, ($recursively ? '' : '/'), true);
		} catch (CannotGetBucket $e) {
			throw new IO\NotFoundException("[{$this->bucket}]:" . $remote);
		}
	}

	protected function normalizePath(string $path): string {
		while (substr($path, 0, 1) == '.' and substr($path, 1, 1) !== '.') {
			$path = ltrim(substr($path, 1), '/');
		}
		return ltrim($path, '/');
	}
}