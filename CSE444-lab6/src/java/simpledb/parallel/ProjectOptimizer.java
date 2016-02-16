package simpledb.parallel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbIterator;
import simpledb.HashEquiJoin;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.OrderBy;
import simpledb.Project;
import simpledb.TransactionId;
import simpledb.TupleDesc;
import simpledb.TupleDesc.TDItem;
import simpledb.Type;

/**
 * Push down projects to minimize data transmission.
 * 
 * For example:
 *    
 *    select movie.id,genre.genre from movie,genre where movie.id=genre.mid
 *    
 *    Only movie.id,genre.mid and genre.genre need to be transmitted through shuffle.
 *    
 * */
public class ProjectOptimizer extends ParallelQueryPlanOptimizer {

    public ProjectOptimizer() {
	super();
    }

    public ProjectOptimizer(ParallelQueryPlanOptimizer next) {
	super(next);
    }

    @Override
    protected void doMyOptimization(TransactionId tid, ParallelQueryPlan plan) {
	CollectProducer masterPlan = plan.getMasterWorkerPlan();
	HashSet<String> needed = new HashSet<String>();
	Iterator<TDItem> it = masterPlan.getTupleDesc().iterator();
        
	while (it.hasNext())
	    needed.add(it.next().fieldName);
	plan.setMasterWorkerPlan((CollectProducer) (pushDownProject(masterPlan,
	        needed)));
    }

    private DbIterator pushDownProject(DbIterator rootPlan,
	    HashSet<String> neededField) {
	if (!(rootPlan instanceof Operator))
	    return rootPlan;
	Operator root = (Operator) rootPlan;
	DbIterator[] children = ((Operator) root).getChildren();

	if (root instanceof CollectProducer || root instanceof ShuffleProducer) {
	    HashSet<String> oldNeeded = new HashSet<String>(neededField);
	    children[0] = pushDownProject(children[0], neededField);
	    TupleDesc newCTD = children[0].getTupleDesc();

		ArrayList<Integer> fields = new ArrayList<Integer>();
		ArrayList<Type> types = new ArrayList<Type>();

		for (String f : oldNeeded) {
		    int i=-1;
		   try
		   {
		    i = newCTD.fieldNameToIndex(f);
		    } catch (NoSuchElementException e)
		   {}
		   if (i>=0)
		   {
		    fields.add(i);
		    types.add(newCTD.getFieldType(i));
		   }
		}
		
		    if (types.size() < newCTD.numFields()) {// some fields are not
                        // needed, project it
                        // out

		Project p = new Project(fields, types, children[0]);

		root.setChildren(new DbIterator[] { p });
		return root;
	    } else {
		children[0] = pushDownProject(children[0], neededField);
		root.setChildren(children);
		return root;
	    }
	} else {

	    if (root instanceof Join || root instanceof HashEquiJoin) {
		String f1Name = null;
		String f2Name = null;
		JoinPredicate jp = null;
		if (root instanceof Join) {
		    Join j = ((Join) root);
		    jp = j.getJoinPredicate();
		    int f1 = jp.getField1();
		    int f2 = jp.getField2();
		    f1Name = children[0].getTupleDesc().getFieldName(f1);
		    f2Name = children[1].getTupleDesc().getFieldName(f2);
		} else {
		    HashEquiJoin j = ((HashEquiJoin) root);
		    jp = j.getJoinPredicate();
		    int f1 = jp.getField1();
		    int f2 = jp.getField2();
		    f1Name = children[0].getTupleDesc().getFieldName(f1);
		    f2Name = children[1].getTupleDesc().getFieldName(f2);
		}

		HashSet<String> left = new HashSet<String>(neededField);
		HashSet<String> right = new HashSet<String>(neededField);

		Iterator<TDItem> leftFields = children[0].getTupleDesc()
		        .iterator();
		HashSet<String> leftPossible = new HashSet<String>();
		while (leftFields.hasNext())
		    leftPossible.add(leftFields.next().fieldName);

		Iterator<String> leftIt = left.iterator();
		while (leftIt.hasNext())
		    if (!leftPossible.contains(leftIt.next()))
			leftIt.remove();

		left.add(f1Name);

		Iterator<TDItem> rightFields = children[1].getTupleDesc()
		        .iterator();
		HashSet<String> rightPossible = new HashSet<String>();
		while (rightFields.hasNext())
		    rightPossible.add(rightFields.next().fieldName);

		Iterator<String> rightIt = right.iterator();
		while (rightIt.hasNext())
		    if (!rightPossible.contains(rightIt.next()))
			rightIt.remove();

		right.add(f2Name);

		children[0] = pushDownProject(children[0], left);
		children[1] = pushDownProject(children[1], right);
		jp = new JoinPredicate(children[0].getTupleDesc()
		        .fieldNameToIndex(f1Name), jp.getOperator(),
		        children[1].getTupleDesc().fieldNameToIndex(f2Name));
		if (root instanceof Join) {
		    return new Join(jp, children[0], children[1]);
		} else {
		    return new HashEquiJoin(jp, children[0], children[1]);
		}

	    } else if (root instanceof Project) {
		Project p = (Project) root;
		Iterator<TDItem> it = p.getTupleDesc().iterator();
		while (it.hasNext())
		    neededField.add(it.next().fieldName);
		children[0] = pushDownProject(children[0], neededField);
//		if (children[0].getTupleDesc().numFields() == p.getTupleDesc()
//		        .numFields())
//		    // This project is not needed
//		    return children[0];
//		else {
		    TupleDesc oldTD = p.getTupleDesc();
		    TupleDesc newTD = children[0].getTupleDesc();
		    ArrayList<Integer> fieldIndex = new ArrayList<Integer>();
		    ArrayList<Type> fieldType = new ArrayList<Type>();
		    Iterator<TDItem> oldIT = oldTD.iterator();
		    while (oldIT.hasNext()) {
			TDItem tdItem = oldIT.next();
			fieldIndex
			        .add(newTD.fieldNameToIndex(tdItem.fieldName));
			fieldType.add(tdItem.fieldType);
		    }
		    Project pp = new Project(fieldIndex, fieldType, children[0]);
		    return pp;
//		}
	    } else if (root instanceof OrderBy) {
		OrderBy o = (OrderBy) root;
		neededField.add(o.getTupleDesc().getFieldName(
		        o.getOrderByField()));
		children[0] = pushDownProject(children[0], neededField);
		o = new OrderBy(children[0].getTupleDesc().fieldNameToIndex(
		        o.getOrderFieldName()), o.isASC(), children[0]);
		return o;
	    } else if (root instanceof Aggregate) {
		Aggregate a = (Aggregate) root;
		String aggregateFieldInInputTuples = a.getChildren()[0].getTupleDesc().getFieldName(a.aggregateField());
		neededField.add(aggregateFieldInInputTuples);
		String g = a.groupFieldName();
		if (g != null)
		    neededField.add(g);
		children[0] = pushDownProject(children[0], neededField);
		TupleDesc newTD = children[0].getTupleDesc();
		int gField = Aggregator.NO_GROUPING;
		int aField = newTD.fieldNameToIndex(aggregateFieldInInputTuples);
		if (g != null)
		    gField = newTD.fieldNameToIndex(g);
		a = new Aggregate(children[0], aField, gField, a.aggregateOp());
		return a;
	    }
	    children[0] = pushDownProject(children[0], neededField);
	    root.setChildren(children);
	    return root;

	}

    }

}
