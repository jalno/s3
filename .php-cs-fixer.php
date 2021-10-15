<?php

$finder = PhpCsFixer\Finder::create()
    ->in([__DIR__ . "/libraries"]);

$config = new PhpCsFixer\Config();
return $config->setRules([
        '@Symfony' => true,
    ])
    ->setFinder($finder)
;