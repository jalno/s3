<?php

namespace packages\s3;

use packages\base\IO\Directory as BaseDirectory;
use packages\base\view\Error;
use packages\s3\File as S3File;

class Directory extends BaseDirectory
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

    public function size(): int
    {
        return $this->getDriver()->directorySize($this->getPath());
    }

    public function move(BaseDirectory $dest): bool
    {
        $result = $this->copyTo($dest);
        if ($result) {
            $this->delete();
        }

        return $result;
    }

    public function rename(string $newName): bool
    {
        $newDirectory = $this->getDirectory()->directory($newName);

        return $this->move($newDirectory);
    }

    /**
     * @return void
     */
    public function delete()
    {
        $this->getDriver()->directoryDelete($this->getPath());
    }

    public function make(): bool
    {
        return $this->getDriver()->mkdir($this->getPath());
    }

    /**
     * @return S3File[]
     */
    public function files(bool $recursively = false): array
    {
        $rawFiles = $this->getDriver()->directoryFiles($this->getPath(), $recursively);

        return array_map(
            function (array $item) {
                $file = new S3File($item['name']);
                $file->setDriver($this->getDriver());

                return $file;
            },
            $rawFiles
        );
    }

    /**
     * @return self[]
     */
    public function directories(bool $recursively = true): array
    {
        $rawDirectories = $this->getDriver()->directoryDirectories($this->getPath(), $recursively);

        return array_map(function (string $path) {
            $file = new self($path);
            $file->setDriver($this->getDriver());

            return $file;
        }, $rawDirectories);
    }

    /**
     * @return array<self|S3File>
     */
    public function items(bool $recursively = true): array
    {
        $rawItems = $this->getDriver()->directoryItems($this->getPath(), $recursively);

        return array_map(function (array $item) {
            $name = $item['name'] ?? $item['prefix'] ?? '';
            $node = ('/' !== substr($name, -1)) ?
                new S3File($name) :
                new self($name);
            $node->setDriver($this->getDriver());

            return $node;
        }, $rawItems);
    }

    public function exists(): bool
    {
        return $this->getDriver()->directoryExists($this->getPath());
    }

    /**
     * @return S3File
     */
    public function file(string $name)
    {
        $file = new S3File($this->getPath().'/'.$name);
        $file->setDriver($this->getDriver());

        return $file;
    }

    /**
     * @return self
     */
    public function directory(string $name)
    {
        $directory = new self($this->getPath().'/'.$name);
        $directory->setDriver($this->getDriver());

        return $directory;
    }

    /**
     * @return self
     */
    public function getDirectory()
    {
        $directory = new self($this->directory);
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
