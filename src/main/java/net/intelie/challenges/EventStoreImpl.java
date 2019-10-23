package net.intelie.challenges;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventStoreImpl implements EventStore {

	/**
	 * Concurrent map used to provide thread-safe operations while allowing multiple
	 * threads to manipulate it
	 * 
	 * @author nkzren
	 */
	private final ConcurrentMap<String, List<Event>> events = new ConcurrentHashMap<String, List<Event>>();

	Logger LOGGER = Logger.getLogger(EventStoreImpl.class.getName());

	@Override
	public void insert(Event event) {
		synchronized (this) { // Adding synchronized block to ensure thread safety on insertion
			try {
				
				LOGGER.log(Level.INFO, "Inserting new event of type: " + event.type());
				if (!events.containsKey(event.type())) {
					List<Event> list = new ArrayList<Event>();
					events.put(event.type(), list);
				}
				events.get(event.type()).add(event);
				
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Error trying to insert event");
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
	}

	@Override
	public void removeAll(String type) {
		try {
			LOGGER.log(Level.INFO, "Removing all events of type: " + type);
			events.remove(type);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Error trying to remove events of type: " + type);
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		List<Event> result = filterByInterval(events.get(type), startTime, endTime);

		return new EventIteratorImpl(result, events);
	}

	/**
	 * Filters the provided list by the interval set on startTime and endTime
	 * 
	 * @param list      Event list to be filtered
	 * @param startTime
	 * @param endTime
	 * @return the result list
	 */
	private List<Event> filterByInterval(List<Event> list, long startTime, long endTime) {
		int startIndex = getIndexStart(list, startTime);
		int endIndex = getIndexEnd(list, endTime);

		if (startIndex <= endIndex) {
			list.subList(startIndex, endIndex + 1);
		} else {
			list.clear();
		}

		return list;
	}

	/**
	 * Binary searches for the first index after the start time
	 * 
	 * @param list
	 * @param start
	 * @return the result index
	 */
	private int getIndexStart(List<Event> list, long start) {
		int low = 0;
		int high = list.size() - 1;
		while (low <= high) {
			int mid = (low + high) / 2;
			if (list.get(mid).timestamp() >= start) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return low;
	}

	/**
	 * Binary searches for the first index before the end time
	 * 
	 * @param list
	 * @param end
	 * @return the result index
	 */
	private int getIndexEnd(List<Event> list, long end) {
		int low = 0;
		int high = list.size() - 1;
		while (low <= high) {
			int mid = (low + high) / 2;
			if (list.get(mid).timestamp() < end) {
				low = mid + 1;
			} else {
				high = mid - 1;
			}
		}
		return high;
	}

}
