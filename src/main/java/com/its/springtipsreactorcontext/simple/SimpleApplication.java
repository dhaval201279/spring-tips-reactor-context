package com.its.springtipsreactorcontext.simple;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
@Slf4j
@RestController
public class SimpleApplication {

    public static void main (String [] args) {
        SpringApplication.run(SimpleApplication.class, args);

    }

    private static Scheduler SCHEDULER = Schedulers.fromExecutor(Executors.newFixedThreadPool(10));

    private static <T> Flux<T> prepare (Flux<T> in) {
        log.info("Entering and leaving SimpleApplication : prepare");
        return in
                .doOnNext(t -> log.info("Inside SimpleApplication : prepare -> accepts with parameter as {} " , t))
                .subscribeOn(SCHEDULER);
    }

    Flux <String> read() {
        log.info("Entering and leaving SimpleApplication : read");
        Flux<String> letters = prepare(Flux.just("A", "B", "C"));
        log.info("Prepared flux of letters");
        Flux<Integer> numbers = prepare(Flux.just(1,2,3));
        log.info("Prepared flux of numbers");
        log.info("Combining flux of letters and numbers");
        /**
         * One way of using context for
         *
         * e.g. ReactorContextWebFilter of Spring Security
         */
        return prepare (Flux
                            .zip(letters, numbers)
                            .map(tuple -> tuple.getT1() + ":" + tuple.getT2())
                            .doOnEach(signal -> {
                                if (! signal.isOnNext()) {
                                    return;
                                }
                                Context context = signal.getContext();
                                Object userId = context.get("userId");
                                log.info("user id for this pipeline stage for data {} is userId {}", signal.get(), userId);
                            })
                            .subscriberContext(Context.of("userId", UUID.randomUUID().toString()))
            );

        /**
         * Another way of using context for
         */
        /*return prepare (Flux
                            .zip(letters, numbers)
                            .map(tuple -> tuple.getT1() + ":" + tuple.getT2())
                            .subscriberContext(new Function<Context, Context>() {
                                @Override
                                public Context apply(Context context) {
                                    return null;
                                }
                            })
            );*/
    }
    @GetMapping("/data")
    Flux<String> get() {
        log.info("Entering and leaving SimpleApplication : get after invoking read");
        return read();
    }

    @Data
    static class UserId {
        private String uid;
    }
}
