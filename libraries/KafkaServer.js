const KafkaNode = require( 'kafka-node' );

class KafkaServer {
    consumer() {
        // const Consumer = Kafka.Consumer;
        // const Client = new Kafka.KafkaClient( {
        // 	kafkaHost: '149.129.221.137:9092'
        // } );
        // const consumer_kafka_client = new Consumer(
        // 	Client,
        // 	[
        // 		{
        // 			topic: 'INS_EMPLOYEE', 
        // 			partition: 0
        // 		}
        // 	],
        // 	{
        // 		autoCommit: true,
        // 		fetchMaxWaitMs: 1000,
        // 		fetchMaxBytes: 1024 * 1024,
        // 		encoding: 'utf8',
        // 		fromOffset: false
        // 	}
        // );
        // console.log( 'Ihsan' );
        // consumer_kafka_client.on( 'message', async function( message ) {
		// 		// var value = message.value.split( "|" );
		// 		// var data = JSON.parse( value[1] );
        //     console.log( message );
        // })
        // consumer_kafka_client.on( 'error', function( err ) {
        //     console.log( 'error', err );
        // });
        const KafkaConsumer = KafkaNode.Consumer;
        const Client = new KafkaNode.KafkaClient( {
            kafkaHost: '149.129.221.137:9092'
        } );
        const Consumer = new KafkaConsumer(
            Client,
            [
                {
                    topic: 'INS_EMPLOYEE', 
                    partition: 0 
                }
            ],
            {
                autoCommit: true,
                fetchMaxWaitMs: 1000,
                fetchMaxBytes: 1024 * 1024,
                encoding: 'utf8',
                fromOffset: false
            }
        );
        Consumer.on('message', async function( message ) {
            var value = message.value.split( "|" );
            var data = JSON.parse( value[1] );
            console.log( typeof data.INSERT_TIME );
            console.log( data.EBCC_VALIDATION_CODE );
            //message = message.toString().split( '|' );
            console.log( "Result Split --------------------------------" );
        })
        Consumer.on('error', function(err) {
            console.log('error', err);
        });
    }
    producer( topic, messages ) {
        const Producer = Kafka.Producer;
        const Client = new Kafka.KafkaClient( {
           kafkaHost: '149.129.221.137:9092' 
        } );

        const producerClient = new Producer( Client );
        const payloads = [
            {
                topic,
                messages: messages
            }
        ];
        producerClient.on( 'ready', async function() {
            const pushStatus = producerClient.send( payloads, ( err,  data ) => {
                if ( err ) {
                    console.log( '[KAFKA PRODUCER] - Broker Update Failed.' );
                } else {
                    console.log( '[KAFKA PRODUCER] - Broker Update Success.' );
                }
            } );
        } );
    
        producerClient.on( 'error', ( err ) => {
            console.log( err );
        } );
    }
}
module.exports = new KafkaServer();
