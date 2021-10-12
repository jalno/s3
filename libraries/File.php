<?php
namespace packages\s3_filesystem;

use packages\s3_api\{Connector, Configuration};
use packages\s3_filesystem\{Driver, Directory as S3Directory};
use packages\base\{view\Error, IO\File as BaseFile, IO\Directory, IO\ReadException};

class File extends BaseFile {

	protected ?Driver $driver = null;

	public function setDriver(Driver $driver): void {
		$this->driver = $driver;
	}
	public function getDriver(): Driver {
		if (empty($this->driver)) {
			$config = Driver::getConfigurationByName();
			if ($config) {
				$this->driver = new Driver($config['configuration'], $config['bucket']);
			}
		}
		if (empty($this->driver)) {
			throw new Error('packages.s3_filesystem.File.getDriver.driver_not_set');
		}
		return $this->driver;
	}

	public function copyTo(BaseFile $dest): bool {
		$driver = $this->getDriver();
		if ($dest instanceof BaseFile\Local) {
			return $driver->download($this->getPath(), $dest);
		} else {
			$tmp = new BaseFile\TMP();
			if ($this->copyTo($tmp)) {
				return $tmp->copyTo($dest);
			}
		}
		return false;
	}
	/**
	 * @return void
	 */
	public function delete() {
		$this->getDriver()->unlink($this->getPath());
	}
	public function rename(string $newName): bool {
		$tmp = new BaseFile\TMP();
		$driver = $this->getDriver();
		$driver->download($this->getPath(), $tmp);
		$newFile = new self($this->directory . '/' . $newName);
		$newFile->setDriver($driver);
		$result = $newFile->write($tmp->read());
		if ($result) {
			$this->delete();
		}
		return $result;
	}
	public function move(BaseFile $dest): bool {
		if ($dest instanceof self) {
			return $this->rename($dest->basename);
		}
		$result = $this->copyTo($dest);
		if ($result) {
			$this->delete();
		}
		return $result;
	}
	public function read(int $length = 0): string {
		if ($length == 0) {
			return $this->getDriver()->get_contents($this->getPath());
		}
		return $this->getDriver()->get_contents($this->getPath(), 0, $length);
	}
	public function write(string $data): bool {
		return $this->getDriver()->put_contents($this->getPath(), $data);
	}
	public function size(): int {
		return $this->getDriver()->size($this->getPath());
	}
	public function exists(): bool {
		return $this->getDriver()->exists($this->getPath());
    }
	public function getDirectory(): S3Directory {
		$directory = new S3Directory($this->directory);
		$directory->setDriver($this->getDriver());
		return $directory;
	}

    public function serialize(): string {
		$data = array(
			'directory' => $this->directory,
			'basename' => $this->basename,
			'driver' => null,
		);

		$driver = $this->getDriver();
		$data['driver'] = array(
			'bucket' => $driver->getBucket(),
			'configuration' => $driver->getConfiguration(),
		);

        return serialize($data);
    }
    public function unserialize($data): void {
		$data = unserialize($data);

		$this->directory = $data['directory'] ?? null;
		$this->basename = $data['basename'] ?? null;

		if ($data['driver'] and
			isset($data['driver']['bucket']) and
			isset($data['driver']['configuration'])
		) {
			$this->driver = new Driver($data['driver']['configuration'], $data['driver']['bucket']);
		}
    }
}