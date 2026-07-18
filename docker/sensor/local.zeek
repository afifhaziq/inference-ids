@load packages/zeek-kafka

redef Kafka::topic_name = "zeek-flows";
redef Kafka::tag_json = T;
redef use_conn_size_analyzer = T;
# The Kafka writer reads broker connection settings from this global, not from a
# filter's own $config table (confirmed against a real build: with only a per-filter
# $config set, librdkafka fell back to the plugin's "localhost:9092" default and
# every produce attempt failed with "Connection refused" to 127.0.0.1:9092).
redef Kafka::kafka_conf = table(
	["metadata.broker.list"] = "backend:9092"
);

# Zeek's AF_PACKET capture on eth0 sees both incoming and locally-generated outgoing
# traffic (that's what lets tcpreplay and Zeek share one interface with no bridge/veth)
# -- but that also means Zeek captures its own librdkafka producer connection to
# backend:9092 as if it were monitored traffic. Veto logging it at all via Conn::LOG's
# policy hook (Zeek 6.x's mechanism for this -- the older zeek-kafka README's $pred
# field on Log::Filter no longer exists on this Zeek version's Log::Filter record,
# confirmed against the actual installed base/frameworks/logging/main.zeek). Checking
# both orig_p and resp_p is required: for connections where Zeek's capture starts
# mid-stream (never saw the SYN), it guesses direction heuristically and sometimes
# assigns the broker's :9092 side as the "originator" instead of the "responder" --
# confirmed against a real build, where a resp_p-only check let one such connection
# through.
hook Conn::log_policy(rec: Conn::Info, id: Log::ID, filter: Log::Filter)
	{
	if ( rec$id$resp_p == 9092/tcp || rec$id$orig_p == 9092/tcp )
		break;
	}

# Only Conn::LOG goes to Kafka. logs_to_send is documented as mutually exclusive with
# per-filter predicates, so this uses Log::add_filter directly rather than
# Kafka::logs_to_send + a global Log::default_writer redef -- the latter (the original,
# naive approach) sends every log stream to Kafka, not just Conn::LOG, and collides with
# this filter's own "conn" path (Zeek renames it to "conn-2" to avoid the clash); both
# confirmed against a real build, not assumed.
event zeek_init() &priority=-10
	{
	Log::add_filter(Conn::LOG, [
		$name = "kafka-conn",
		$writer = Log::WRITER_KAFKAWRITER
	]);
	}
