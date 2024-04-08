<?php

namespace packages\s3;

use packages\base\IO\File as BaseFile;
use packages\base\view\Error;
use packages\s3\Directory as S3Directory;

class File extends BaseFile
{
    protected ?Driver $driver = null;

    public function setDriver(Driver $driver): void
    {
        $this->driver = $driver;
    }

    public function getDriver(): Driver
    {
        if (empty($this->driver)) {
            $config = Driver::getConfigurationByName();
            if ($config) {
                $this->driver = new Driver($config['configuration'], $config['bucket']);
            }
        }
        if (empty($this->driver)) {
            throw new Error('packages.s3.File.getDriver.driver_not_set');
        }

        return $this->driver;
    }

    public function copyTo(BaseFile $dest): bool
    {
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

    public function copyFrom(BaseFile $source): bool
    {
        if ($source instanceof BaseFile\Local) {
            return $this->getDriver()->upload($source, $this->getPath());
        } else {
            $tmp = new BaseFile\TMP();
            if ($source->copyTo($tmp)) {
                return $this->copyFrom($source);
            }
        }

        return false;
    }

    /**
     * @return void
     */
    public function delete()
    {
        $this->getDriver()->unlink($this->getPath());
    }

    public function rename(string $newName): bool
    {
        $tmp = new BaseFile\TMP();
        $driver = $this->getDriver();
        $driver->download($this->getPath(), $tmp);
        $newFile = new self($this->directory.'/'.$newName);
        $newFile->setDriver($driver);
        $result = $newFile->write($tmp->read());
        if ($result) {
            $this->delete();
        }

        return $result;
    }

    public function move(BaseFile $dest): bool
    {
        if ($dest instanceof self) {
            return $this->rename($dest->basename);
        }
        $result = $this->copyTo($dest);
        if ($result) {
            $this->delete();
        }

        return $result;
    }

    public function read(int $length = 0): string
    {
        if (0 == $length) {
            return $this->getDriver()->get_contents($this->getPath());
        }

        return $this->getDriver()->get_contents($this->getPath(), 0, $length);
    }

    public function write(string $data): bool
    {
        return $this->getDriver()->put_contents($this->getPath(), $data);
    }

    public function size(): int
    {
        return $this->getDriver()->size($this->getPath());
    }

    public function exists(): bool
    {
        return $this->getDriver()->exists($this->getPath());
    }

    public function getDirectory(): S3Directory
    {
        $directory = new S3Directory($this->directory);
        $directory->setDriver($this->getDriver());

        return $directory;
    }

    /**
     * @return array{directory:string,basename:string,driver:array{bucket:string,configuration:\packages\s3_api\Configuration}}
     */
    public function __serialize(): array
    {
        $driver = $this->getDriver();
        return [
            'directory' => $this->directory,
            'basename' => $this->basename,
            'driver' => [
                'bucket' => $driver->getBucket(),
                'configuration' => $driver->getConfiguration(),
            ],
        ];
    }

    /**
     * @param array{directory?:string,basename?:string,driver?:array{bucket?:string,configuration?:\packages\s3_api\Configuration}} $data
     */
    public function __unserialize(array $data): void
    {
        if (isset($data['directory'])) {
            $this->directory = $data['directory'];
        }
        if (isset($data['basename'])) {
            $this->basename = $data['basename'];
        }

        if (isset($data['driver']) and
            isset($data['driver']['bucket']) and
            isset($data['driver']['configuration'])
        ) {
            $this->driver = new Driver($data['driver']['configuration'], $data['driver']['bucket']);
        }
    }
}
