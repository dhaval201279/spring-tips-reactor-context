package com.its.springtipsreactorcontext.mdc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.util.context.Context;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootApplication
//@Slf4j
@Log4j2
@RestController
public class MdcApplication {

    private final RestaurantService restaurantService;

    private final static String UID = "uid";

    public MdcApplication(RestaurantService restaurantService) {

        this.restaurantService = restaurantService;
    }

    public static void main(String [] args) {
        log.info("Entering and leaving MdcApplication : main");
        SpringApplication.run(MdcApplication.class, args);
    }

    @GetMapping("/{uid}/restaurants/{price}")
    Flux<Restaurant> restaurantFlux(@PathVariable String uid, @PathVariable Double price) {
        log.info("Entering MdcApplication : restaurantFlux");
        return adaptResult(this.restaurantService.getByMaxPrice(price), uid, price);
    }
    Flux<Restaurant> adaptResult(Flux<Restaurant> in, String uid, Double price) {
        log.info("Entering and leaving MdcApplication : adaptResult with uid {} and price {} ", uid, price);
        return Mono
                .just(String.format("Finding restaurants having price lower than $%.2f for %s", price, uid))
                .doOnEach(logOnNext(log::info))
                .thenMany(in)
                .doOnEach(logOnNext( r -> log.info("Found restaurant {} for $ {} ", r.getName(), r.getPricePerPerson())))
                .subscriberContext(Context.of(UID, uid))
                ;
    }
    private static <T> Consumer<Signal<T>> logOnNext(Consumer<T> logStatement) {
        log.info("Entering and leaving MdcApplication : logOnNext");
        return signal -> {
            if (! signal.isOnNext()) {
                log.info("Signal is not next !!!!! ");
                return;
            }
            log.info("Signal is next....hence fetching UID from context ");
            Optional<String> uidOptional = signal.getContext().getOrEmpty(UID);

            Runnable orElse = () -> {
                log.info("Within orElse - Instantiating runnable to allow logStatement to accept signal");
                logStatement.accept(signal.get());
                log.info("Exiting orElse . . . .");
            };

            Consumer<String> ifPresent = uid -> {
                log.info("Within ifPresent - Putting uid {} in MDC's putCloseable ", uid);
                try (MDC.MDCCloseable closeable = MDC.putCloseable(UID, uid)) {
                    log.info("About to execute orElse.run() ");
                    orElse.run();
                    log.info("orElse.run() completed");
                }
                log.info("Exiting ifPresent");
            };


            uidOptional.ifPresentOrElse(ifPresent, orElse);
        };
    }
}

@Service
@Slf4j
class RestaurantService {

    private final Collection<Restaurant> restaurants = new ConcurrentSkipListSet<Restaurant>(
            (o1, o2) -> {
                Double one = o1.getPricePerPerson();
                Double two = o2.getPricePerPerson();
                return one.compareTo(two);
            }
    );

    public RestaurantService() {
        log.info("Entering empty constructor of RestaurantService after initializing integer stream");
        IntStream
            .range(0,1000)
            .mapToObj(Integer::toString)
            .map(i -> "Restaurant # " + i)
            .map(str -> new Restaurant(new Random().nextDouble() * 100  , str))
            .forEach(this.restaurants::add);
    }

    Flux<Restaurant> getByMaxPrice(double maxPrice) {
        log.info("Entering RestaurantService : getByMaxPrice with maxPrice as {} ", maxPrice);
        Stream<Restaurant> res = this.restaurants
                                    .parallelStream()
                                    .filter(restaurant -> restaurant.getPricePerPerson() <= maxPrice);
        log.info("Return flux from stream of restaurants {}", res);
        return Flux.fromStream(res);
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class Restaurant {
    private double pricePerPerson;
    private String name;
}
