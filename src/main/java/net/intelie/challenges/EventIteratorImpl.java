package net.intelie.challenges;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class EventIteratorImpl implements EventIterator {
	
	private final ConcurrentMap<String, List<Event>> map;

	private List<Event> iterator;
	private Event curElem;
	private int curPos;

	EventIteratorImpl(List<Event> iterator, ConcurrentMap<String, List<Event>> map) {
		this.map = map;
		this.iterator = iterator;
	}

	@Override
	public void close() throws Exception {
		iterator = null;
	}

	@Override
	public boolean moveNext() {
		if (iterator != null && curPos < iterator.size()) {
			this.curElem = iterator.get(this.curPos++);
		} else {
			this.curElem = null;
		}
		return curElem != null;
	}

	@Override
	public Event current() {
		return curElem;
	}

	@Override
	public void remove() {
		curPos--;
		map.get(curElem.type()).remove(curPos);
	}

}
