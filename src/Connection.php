<?php
namespace evseevnn\Cassandra;
use evseevnn\Cassandra\Cluster\Node;
use evseevnn\Cassandra\Enum;
use evseevnn\Cassandra\Exception\ConnectionException;
use evseevnn\Cassandra\Protocol\Frame;
use evseevnn\Cassandra\Protocol\Request;
use evseevnn\Cassandra\Protocol\Response;

class Connection {

	/**
	 * @var Cluster
	 */
	private $cluster;

	/**
	 * @var Node
	 */
	private $node;

	/**
	 * @var resource
	 */
	private $connection;

	/**
	 * @param Cluster $cluster
	 */
	public function __construct(Cluster $cluster) {
		$this->cluster = $cluster;
	}

	/**
	 * @param array $options
	*/
	public function connect(array $options = []) {
		try {
			$this->node = $this->cluster->getRandomNode();
			$this->connection = $this->node->getConnection($options);
		} catch (ConnectionException $e) {
			$this->connect();
		}
	}

	/**
	 * @return bool
	 */
	public function disconnect() {
		return fclose($this->connection);
	}

	/**
	 * @return bool
	 */
	public function isConnected() {
		return $this->connection !== null;
	}

	/**
	 * @param Request $request
	 * @return \evseevnn\Cassandra\Protocol\Response
	 */
	public function sendRequest(Request $request) {
		$frame = new Frame(Enum\VersionEnum::REQUEST, $request->getType(), $request);
		if (@fwrite($this->connection, $frame) === FALSE) {
			throw new ConnectionException("Connection error: fwrite");
		}
		return $this->getResponse();
	}

	/**
	 * @param $length
	 * @throws Exception\ConnectionException
	 * @return string
	 */
	private function fetchData($length) {
		$data = "";
		$length_left = $length;
		while ($length_left > 0) {
			$data_slice = @fread($this->connection, $length_left);
			if ($data_slice === FALSE || $data_slice === "") {
				throw new ConnectionException("Connection error: fread");
			}
			$data .= $data_slice;
			$length_left -= strlen($data_slice);
		}

		return $data;
	}

	private function getResponse() {
		$data = $this->fetchData(8);
		$data = unpack('Cversion/Cflags/cstream/Copcode/Nlength', $data);
		if ($data['length']) {
			$body = $this->fetchData($data['length']);
		} else {
			$body = '';
		}

		return new Response($data['opcode'], $body);
	}

	/**
	 * @return \evseevnn\Cassandra\Cluster\Node
	 */
	public function getNode() {
		return $this->node;
	}
}
