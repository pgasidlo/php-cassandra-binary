<?php
namespace evseevnn\Cassandra\Cluster;

use evseevnn\Cassandra\Exception\ConnectionException;

class Node {

	/**
	 * @var string
	 */
	private $host;

	/**
	 * @var int
	 */
	private $port = 9042;

	/**
	 * @var resource
	 */
	private $socket;

	/**
	 * @var array
	 */
	private $options = [
		'timeout' => 1,
		'username' => null,
		'password' => null
	];

	/**
	 * @param string $host
	 * @param array $options
	 * @throws \InvalidArgumentException
	 */
	public function __construct($host, array $options = []) {
		$this->host = $host;
		if (strstr($this->host, ':')) {
			$this->port = (int)substr(strstr($this->host, ':'), 1);
			$this->host = substr($this->host, 0, -1 - strlen($this->port));
			if (!$this->port) {
				throw new \InvalidArgumentException('Invalid port number');
			}
		}
		$this->options = array_merge($this->options, $options);
	}

	/**
	 * @return resource
	 * @param array $options
	 * @throws \Exception
	 */
	public function getConnection(array $options = []) {
		if (is_resource($this->socket)) return $this->socket;

		$options = array_merge($this->options, $options);

		$timeout["sec"] = (int)$options['timeout'];
		$timeout["usec"] = ($options['timeout'] - (int)$options['timeout']) * 1000000;

		$errno = $errstr = null;
		if (!($this->socket = fsockopen($this->host, $this->port, $errno, $errstr, $timeout['sec'] ?: 1))) {
			throw new ConnectionException("Connection error: fsockopen: {$this->host}:{$this->port}");
		}
		stream_set_timeout($this->socket, $timeout['sec'], $timeout['usec']);

		return $this->socket;
	}

	/**
	 * @return array
	 */
	public function getOptions() {
		return $this->options;
	}
}
