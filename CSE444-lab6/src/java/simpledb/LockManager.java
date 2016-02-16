package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LockManager {

	private final ConcurrentMap<PageId, Object> locks;
	private final Map<PageId, List<TransactionId>> sharedLocks;
	private final Map<PageId, TransactionId> exclusiveLocks;
	private final ConcurrentMap<TransactionId, Collection<PageId>> pageIdsLockedByTransaction;
	private final ConcurrentMap<TransactionId, Collection<TransactionId>> dependencyGraph;

	private LockManager() {
		locks = new ConcurrentHashMap<PageId, Object>();
		sharedLocks = new HashMap<PageId, List<TransactionId>>();
		exclusiveLocks = new HashMap<PageId, TransactionId>();
		pageIdsLockedByTransaction = new ConcurrentHashMap<TransactionId, Collection<PageId>>();
		dependencyGraph = new ConcurrentHashMap<TransactionId, Collection<TransactionId>>();
	}

	public static LockManager create() {
		return new LockManager();
	}

	private Object getLock(PageId pageId) {
		locks.putIfAbsent(pageId, new Object());
		return locks.get(pageId);
	}

	public boolean acquireLock(TransactionId transactionId, PageId pageId,
			Permissions permissions) throws TransactionAbortedException {
		TransactionId notNullTransactionId = (transactionId == null) ? TransactionId.NULL_TRANSACTION_ID
				: transactionId;
		if (permissions == Permissions.READ_ONLY) {
			if (hasReadPermissions(notNullTransactionId, pageId)) {
				return true;
			}
			while (!acquireReadOnlyLock(notNullTransactionId, pageId)) {
				// waiting for lock
			}
		} else if (permissions == Permissions.READ_WRITE) {
			if (hasWritePermissions(notNullTransactionId, pageId)) {
				return true;
			}
			while (!acquireReadWriteLock(notNullTransactionId, pageId)) {
				// waiting for lock
			}
		} else {
			throw new IllegalArgumentException(
					"Expected either READ_ONLY or READ_WRITE permissions.");
		}
		addPageToTransactionLocks(notNullTransactionId, pageId);
		return true;
	}

	private boolean hasReadPermissions(TransactionId transactionId,
			PageId pageId) {
		if (hasWritePermissions(transactionId, pageId)) {
			return true;
		}
		return sharedLocks.containsKey(pageId)
				&& sharedLocks.get(pageId).contains(transactionId);
	}

	private boolean hasWritePermissions(TransactionId transactionId,
			PageId pageId) {
		return exclusiveLocks.containsKey(pageId)
				&& transactionId.equals(exclusiveLocks.get(pageId));
	}

	private void addPageToTransactionLocks(TransactionId transactionId,
			PageId pageId) {
		pageIdsLockedByTransaction.putIfAbsent(transactionId,
				new LinkedBlockingQueue<PageId>());
		pageIdsLockedByTransaction.get(transactionId).add(pageId);
	}

	public boolean acquireReadOnlyLock(TransactionId transactionId,
			PageId pageId) throws TransactionAbortedException {
		Object lock = getLock(pageId);
		while (true) {
			synchronized (lock) {
				TransactionId exclusiveLockHolder = exclusiveLocks.get(pageId);
				if (exclusiveLockHolder == null
						|| transactionId.equals(exclusiveLockHolder)) {
					removeDependencies(transactionId);
					addSharedUser(transactionId, pageId);
					return true;
				}
				addDependency(transactionId, exclusiveLockHolder);
			}
		}
	}

	private void removeDependencies(TransactionId dependent) {
		dependencyGraph.remove(dependent);
	}

	private void addDependency(TransactionId dependent, TransactionId dependee)
			throws TransactionAbortedException {
		Collection<TransactionId> dependees = new ArrayList<TransactionId>();
		dependees.add(dependee);
		addDependencies(dependent, dependees);
	}

	private void addDependencies(TransactionId dependent,
			Collection<TransactionId> dependees)
			throws TransactionAbortedException {
		dependencyGraph.putIfAbsent(dependent,
				new LinkedBlockingQueue<TransactionId>());
		Collection<TransactionId> dependeesCollection = dependencyGraph
				.get(dependent);
		boolean addedDependee = false;
		for (TransactionId newDependee : dependees) {
			if (!dependeesCollection.contains(newDependee)
					&& !newDependee.equals(dependent)) {
				addedDependee = true;
				dependeesCollection.add(newDependee);
			}
		}
		if (addedDependee) {
			abortIfDeadlocked();
		}
	}

	private void abortIfDeadlocked() throws TransactionAbortedException {
		Set<TransactionId> visitedTransactionIds = new HashSet<TransactionId>();
		for (TransactionId transactionId : dependencyGraph.keySet()) {
			if (!visitedTransactionIds.contains(transactionId)) {
				testForDeadlock(transactionId, visitedTransactionIds,
						new Stack<TransactionId>());
			}
		}
	}

	private void testForDeadlock(TransactionId transactionId,
			Set<TransactionId> visitedTransactionIds,
			Stack<TransactionId> parents) throws TransactionAbortedException {
		visitedTransactionIds.add(transactionId);
		if (!dependencyGraph.containsKey(transactionId)) {
			return;
		}
		for (TransactionId dependee : dependencyGraph.get(transactionId)) {
			if (parents.contains(dependee)) {
				throw new TransactionAbortedException();
			}
			if (!visitedTransactionIds.contains(dependee)) {
				parents.push(transactionId);
				testForDeadlock(dependee, visitedTransactionIds, parents);
				parents.pop();
			}
		}
	}

	private void addSharedUser(TransactionId transactionId, PageId pageId) {
		if (!sharedLocks.containsKey(pageId)) {
			sharedLocks.put(pageId, new ArrayList<TransactionId>());
		}
		sharedLocks.get(pageId).add(transactionId);
	}

	private Collection<TransactionId> getLockHolders(PageId pageId) {
		Collection<TransactionId> lockHolders = new ArrayList<TransactionId>();
		if (exclusiveLocks.containsKey(pageId)) {
			lockHolders.add(exclusiveLocks.get(pageId));
			return lockHolders;
		}
		if (sharedLocks.containsKey(pageId)) {
			lockHolders.addAll(sharedLocks.get(pageId));
		}
		return lockHolders;
	}

	private boolean isLockedByOthers(TransactionId transactionId,
			Collection<TransactionId> lockHolders) {
		if (lockHolders == null || lockHolders.isEmpty()) {
			return false;
		}
		if (lockHolders.size() == 1
				&& transactionId.equals(lockHolders.iterator().next())) {
			return false;
		}
		return true;
	}

	private void addExclusiveUser(TransactionId transactionId, PageId pageId) {
		exclusiveLocks.put(pageId, transactionId);
	}

	public boolean acquireReadWriteLock(TransactionId transactionId,
			PageId pageId) throws TransactionAbortedException {
		Object lock = getLock(pageId);
		while (true) {
			synchronized (lock) {
				Collection<TransactionId> lockHolders = getLockHolders(pageId);
				if (!isLockedByOthers(transactionId, lockHolders)) {
					removeDependencies(transactionId);
					addExclusiveUser(transactionId, pageId);
					return true;
				}
				addDependencies(transactionId, lockHolders);
			}
		}
	}

	private void releaseLock(TransactionId transactionId, PageId pageId) {
		Object lock = getLock(pageId);
		synchronized (lock) {
			exclusiveLocks.remove(pageId);
			if (sharedLocks.containsKey(pageId)) {
				sharedLocks.get(pageId).remove(transactionId);
			}
		}
	}

	public void releasePage(TransactionId transactionId, PageId pageId) {
		releaseLock(transactionId, pageId);
		if (pageIdsLockedByTransaction.containsKey(transactionId)) {
			pageIdsLockedByTransaction.get(transactionId).remove(pageId);
		}
	}

	public void releasePages(TransactionId transactionId) {
		if (pageIdsLockedByTransaction.containsKey(transactionId)) {
			Collection<PageId> pageIds = pageIdsLockedByTransaction
					.get(transactionId);
			for (PageId pageId : pageIds) {
				releaseLock(transactionId, pageId);
			}
			pageIdsLockedByTransaction.replace(transactionId,
					new LinkedBlockingQueue<PageId>());
		}
	}

	public boolean holdsLock(TransactionId transactionId, PageId pageId) {
		if (!pageIdsLockedByTransaction.containsKey(transactionId)) {
			return false;
		}
		return pageIdsLockedByTransaction.get(transactionId).contains(pageId);
	}
}