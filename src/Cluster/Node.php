<?php
namespace evseevnn\Cassandra\Cluster;

use evseevnn\Cassandra\Exception\ConnectionException;

class Node {

	const STREAM_TIMEOUT_MS = 10;
	const STREAM_TIMEOUT_US = 0;

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
		if (!empty($this->socket)) return $this->socket;

		$options = array_merge($this->options, $options);

		$timeout = ["sec" => self::STREAM_TIMEOUT_MS, "usec" => self::STREAM_TIMEOUT_US];
		if (isset($options['timeout'])) {
			$timeout["sec"] = (int)$options['timeout'];
			$timeout["usec"] = ($options['timeout'] - (int)$options['timeout']) * 1000000;
		}

		$this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		socket_set_option($this->socket, getprotobyname('TCP'), TCP_NODELAY, 1);
		socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, $timeout);
		socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, $timeout);
		if (!socket_connect($this->socket, $this->host, $this->port)) {
			throw new ConnectionException("Unable to connect to Cassandra node: {$this->host}:{$this->port}");
		}

		return $this->socket;
	}

	/**
	 * @return array
	 */
	public function getOptions() {
		return $this->options;
	}
}