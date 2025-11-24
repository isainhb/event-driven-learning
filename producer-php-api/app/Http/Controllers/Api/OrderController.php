<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use RdKafka\Producer;
use RdKafka\Topic;

class OrderController extends Controller
{
    // Define Kafka broker and topic
    const KAFKA_BROKER = 'kafka:29092'; // Kafka's service name and port in docker-compose.yml
    const KAFKA_TOPIC = 'orders_processing';

    public function store(Request $request)
    {
        /* 
        Validate the following structure:
        {
            "order_id": 1001,
            "customer_id": 42,
            "total_amount": 99.99,
            "items": [
                {"sku": "TSHIRT-BLK", "quantity": 1},
                {"sku": "MUG-COF", "quantity": 1}
            ]
        }
        */
        // 1. Validate request
        $validated = $request->validate([
            'order_id' => 'required|integer',
            'customer_id' => 'required|integer',
            'total_amount' => 'required|numeric',
            'items' => 'required|array',
            'items.*.sku' => 'required|string',
            'items.*.quantity' => 'required|integer',
        ]);

        // 2. Create message/event
        $eventPayload = [
            'event_type' => 'OrderCreated',
            'data' => $validated,
            'timestamp' => now()->toDateTimeString(),
        ];

        // 3. Publish event to Kafka
        try {
            // Configure producer
            $conf = new \RdKafka\Conf();
            $conf->set('bootstrap.servers', self::KAFKA_BROKER);

            $producer = new Producer($conf);

            // Define topic
            $topic = $producer->newTopic(self::KAFKA_TOPIC);

            // Publish message (use order_id as key to garantize order processing)
            $message = json_encode($eventPayload);
            $key = (string) $validated['order_id'];

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);

            // Flush to ensure the message is sent before the script ends
            $producer->flush(1000); // Wait until 1000ms to ensure message is sent

            // Quickly API response
            return response()->json([
                'message' => 'Order processed and event published in Kafka',
                'event_id' => $key,
            ], 202); // Accepted, async processing
        } catch (\Exception $e) {
            return response()->json([
                'message' => 'Failed to publish event to Kafka',
                'error' => $e->getMessage(),
            ], 500);
        }
    }
}
