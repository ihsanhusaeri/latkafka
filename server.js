const express = require( 'express' );
const app = express();
const kafkaServer = require( './libraries/KafkaServer.js' );
const bodyParser = require( 'body-parser' )
var KafkaNode = require( 'kafka-node' );

app.use( bodyParser.json() );

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
        autoCommit: false
        // fetchMaxWaitMs: 1000,
        // fetchMaxBytes: 1024 * 1024,
        // encoding: 'utf8',
        // fromOffset: false
    }
);
Consumer.on('message', async function( message ) {
    // var value = message.value.split( "|" );
    // var data = JSON.parse( value[1] );
    // console.log( typeof data.INSERT_TIME );
    // console.log( data.EBCC_VALIDATION_CODE );
    console.log( message );
    console.log( 'Ihsan' );
    //message = message.toString().split( '|' );
    console.log( "Result Split --------------------------------" );
})

app.post( '/api/v1.1/insert-kafka', ( req, res ) => {
    var name = req.body.name;
    var role = req.body.role;
    var address = req.body.address;

    var kafkaBody = {
        NM: name,
        ADRS: address,
        RL: role
    }
    
    kafkaServer.producer( 'INS_EMPLOYEE', JSON.stringify( kafkaBody ) );
    res.send( {
        status: true,
        message: 'KAFKA SUCCESS!'
    } );
} );

// app.get( '/api/v1.1/get-kafka', ( req, res ) => {
    
//     res.send( {
//         status: true,
//         message: 'success!'
//     } );
// } );


app.listen( 3000, () => {
    console.log( 'server listen on port 3000' );
} );