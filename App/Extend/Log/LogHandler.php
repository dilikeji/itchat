<?php

namespace App\Extend\Log;

use EasySwoole\EasySwoole\Config;
use EasySwoole\Log\LoggerInterface;

class LogHandler implements LoggerInterface
{
    private string|null|false $logDir;

    function __construct(string $logDir = null)
    {
        if (empty($logDir)) {
            $logDir = getcwd();
        }
        $this->logDir = $logDir;
    }

    function log(?string $msg, int $logLevel = self::LOG_LEVEL_INFO, string $category = 'debug'): string
    {
        $date = date('Ymd');
        $time = date('Y-m-d H:i:s');
        $levelStr = $this->levelMap($logLevel);
        $filePath = $this->logDir . "/$date.log";
        if (is_file($filePath) && filesize($filePath) >  1024 * 1024 * 1024) {
            $timeStr = date('His');
            $newLogFile = $this->logDir . "/{$date}_$timeStr.log";
            rename($filePath, $newLogFile);
        }
        $str = "[{$time}][{$category}][{$levelStr}]:{$msg}\n";
        file_put_contents($filePath, $str, FILE_APPEND | LOCK_EX);
        return $str;
    }

    private function levelMap(int $level): string
    {
        return match ($level) {
            self::LOG_LEVEL_DEBUG => 'debug',
            self::LOG_LEVEL_INFO => 'info',
            self::LOG_LEVEL_NOTICE => 'notice',
            self::LOG_LEVEL_WARNING => 'warning',
            self::LOG_LEVEL_ERROR => 'error',
            default => 'unknown',
        };
    }

    function console(?string $msg, int $logLevel = self::LOG_LEVEL_INFO, string $category = 'console'): void
    {
        $date = date('Y-m-d H:i:s');
        $levelStr = $this->levelMap($logLevel);
        $temp = "[{$date}][{$category}][{$levelStr}]:[{$msg}]\n";
        fwrite(STDOUT, $temp);
    }
}