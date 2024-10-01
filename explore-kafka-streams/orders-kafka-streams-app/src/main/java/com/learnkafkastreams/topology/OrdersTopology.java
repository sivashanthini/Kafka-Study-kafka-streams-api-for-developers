package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String STORES = "stores";

    static Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
    static Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

    static ValueMapper<Order, Revenue> revenueValueMapper = order -> {
        return new Revenue(order.locationId(), order.finalAmount());
    };
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var orderStream = streamsBuilder.stream(ORDERS, Consumed.with(Serdes.String(), SerdeFactory.orderSerdeGenerics()));
        orderStream.print(Printed.<String, Order>toSysOut().withLabel("Order Stream : "));

        orderStream.split(Named.as("General-Restaurant-Stream"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                    generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("general orders : "));
                    generalOrderStream.mapValues(value -> revenueValueMapper.apply(value))
                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdeFactory.revenueSerdeGenerics()));
                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                    restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("restaurant orders : "));
                    restaurantOrderStream.mapValues(value -> revenueValueMapper.apply(value))
                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdeFactory.revenueSerdeGenerics()));
                }));
        return streamsBuilder.build();
    }
}
