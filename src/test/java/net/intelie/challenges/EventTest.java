package net.intelie.challenges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EventTest {

	EventStoreImpl store = new EventStoreImpl();

	Event event0 = new Event("some_type_A", 0L);
	Event event1 = new Event("some_type_A", 1L);
	Event event2 = new Event("some_type_B", 2L);
	Event event3 = new Event("some_type_B", 3L);
	Event event4 = new Event("some_type_C", 4L);
	Event event5 = new Event("some_type_A", 5L);
	Event event6 = new Event("some_type_A", 6L);

	@Before
	public void initialize() throws Exception {
		store.insert(event0);
		store.insert(event1);
		store.insert(event2);
		store.insert(event3);
		store.insert(event4);
	}

	@After
	public void end() throws Exception {
		store.removeAll("some_type_A");
		store.removeAll("some_type_B");
		store.removeAll("some_type_C");
	}

	@Test
	public void thisIsAWarning() throws Exception {
		Event event = new Event("some_type", 123L);

		// THIS IS A WARNING:
		// Some of us (not everyone) are coverage freaks.
		assertEquals(123L, event.timestamp());
		assertEquals("some_type", event.type());
	}

	@Test
	public void AssertFalse_IteratesMoreThanCollectionSize() {
		EventIterator iterator = store.query("some_type_B", 0L, 7L);

		assertTrue(iterator.moveNext());
		assertEquals(event2, iterator.current());

		assertTrue(iterator.moveNext());
		assertEquals(event3, iterator.current());

		assertFalse(iterator.moveNext());
	}

	@Test
	public void AssertFalse_IteratesRemovedCollection() {
		EventIterator iterator = store.query("some_type_C", 0L, 7L);

		while (iterator.moveNext()) {
			iterator.remove();
		}

		iterator = store.query("some_type_C", 0L, 7L);
		assertFalse(iterator.moveNext());
	}

	@Test
	public void AssertTrue_QueryIntervalCorrect() {
		EventIterator iterator = store.query("some_type_A", 0L, 4L);

		int count = 0;
		while (iterator.moveNext()) {
			count++;
		}
		assertTrue(count == 2);
	}

	@Test
	public void Assert_QueryTypeCorrect() {
		EventIterator iterator = store.query("some_type_B", 0L, 7L);

		while (iterator.moveNext()) {
			assertNotEquals("some_type_A", iterator.current().type());
			assertNotEquals("some_type_C", iterator.current().type());
		}
	}

	@Test
	public void Assert_ThreadSafetyOnInsert() throws Exception {
		int threads = 500;
		ExecutorService service = Executors.newFixedThreadPool(threads);

		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean running = new AtomicBoolean();
		AtomicInteger overlaps = new AtomicInteger();

		Collection<Future<Integer>> futures = new ArrayList<Future<Integer>>(threads);
		for (int t = 0; t < threads; ++t) {
			final int i = t;
			futures.add(service.submit(() -> {
				if (running.get()) {
					overlaps.incrementAndGet();
				}
				running.set(true);
				store.insert(new Event("some_type", (long) i));
				running.set(false);
				return i;
			}));
		}
		
		latch.countDown();
		Set<Integer> ids = new HashSet<Integer>();
		for (Future<Integer> f : futures) {
			ids.add(f.get());
		}
		assertTrue(overlaps.get() > 0);
		EventIterator iterator = store.query("some_type", 0L, 999999L);
		int count = 0;
		while (iterator.moveNext()) {
			count++;
		}
		assertTrue(count == threads);
	}

}