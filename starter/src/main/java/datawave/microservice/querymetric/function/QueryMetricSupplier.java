package datawave.microservice.querymetric.function;

import datawave.microservice.querymetric.QueryMetricUpdate;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.function.Supplier;

public class QueryMetricSupplier implements Supplier<Flux<Message<QueryMetricUpdate>>> {
    private final Sinks.Many<Message<QueryMetricUpdate>> messagingSink = Sinks.many().multicast().onBackpressureBuffer();
    
    public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
        return messagingSink.tryEmitNext(queryMetricUpdate).isSuccess();
    }
    
    @Override
    public Flux<Message<QueryMetricUpdate>> get() {
        return messagingSink.asFlux().subscribeOn(Schedulers.boundedElastic()).share();
    }
}
