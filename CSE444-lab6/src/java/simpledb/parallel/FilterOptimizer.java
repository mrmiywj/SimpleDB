package simpledb.parallel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import simpledb.DbIterator;
import simpledb.Filter;
import simpledb.HashEquiJoin;
import simpledb.Join;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.SeqScan;
import simpledb.TransactionId;
import simpledb.TupleDesc;

/**
 *  This optimizer spreads filters. For example:
 *    
 *    select * from movie,genre where movie.id=genre.mid and movie.id<2;
 *    
 *    In this query, movie has a filter of very high selectivity. Since
 *    movie.id=genre.mid, it's equivalent to add another filter: genre.mid<2, i.e.:
 *    
 *    select * from movie,genre where movie.id=genre.mid and movie.id<2 and genre.mid<2;
 *    
 *    The two queries are equivalent.
 *     
 * */
public class FilterOptimizer extends ParallelQueryPlanOptimizer {

    public FilterOptimizer() {
        super();
    }

    public FilterOptimizer(ParallelQueryPlanOptimizer next) {
        super(next);
    }

    @Override
    protected void doMyOptimization(TransactionId tid,ParallelQueryPlan plan) {
        spreadJoinKeySelection(plan);
        CollectProducer masterPlan =plan.getMasterWorkerPlan();
        plan.setMasterWorkerPlan(masterPlan);
    }

    /**
     * Spread filters
     * */
    private void spreadJoinKeySelection(ParallelQueryPlan plan) {
        CollectProducer masterWorkerPlan = plan.getMasterWorkerPlan();
        HashMap<String, HashSet<String>> eq = new HashMap<String, HashSet<String>>();
        HashMap<String, HashSet<Predicate>> existingFilters = new HashMap<String, HashSet<Predicate>>();
        // newFilters: tableAlias.fieldNameWithoutAlias.PredicateSet
        HashMap<String, HashMap<String, HashSet<Predicate>>> newFilters = new HashMap<String, HashMap<String, HashSet<Predicate>>>();
        getEquivalentJoinFields(masterWorkerPlan, eq, existingFilters);
        for (Entry<String, HashSet<Predicate>> existingFilterEntry : existingFilters
                .entrySet()) {
            String field = existingFilterEntry.getKey();
            HashSet<Predicate> predicates = existingFilterEntry.getValue();
            HashSet<String> equivalent = eq.get(field);
            if (equivalent != null) {// field is a join key
                equivalent.add(field);
                if (equivalent.size() > 1) {// there are equivalent fields
                    for (String equiField : equivalent) {
                        if (!equiField.equals(field)) {
                            int l = equiField.indexOf('.');
                            String tableAlias = equiField.substring(0, l);
                            String fieldName = equiField.substring(l + 1);
                            HashMap<String, HashSet<Predicate>> fieldPred = newFilters
                                    .get(tableAlias);
                            if (fieldPred == null) {
                                fieldPred = new HashMap<String, HashSet<Predicate>>();
                                newFilters.put(tableAlias, fieldPred);
                            }
                            HashSet<Predicate> preds = fieldPred.get(fieldName);
                            if (preds == null) {
                                preds = new HashSet<Predicate>();
                                fieldPred.put(fieldName, preds);
                            }
                            preds.addAll(predicates);
                        }
                    }
                }
            }
        }
        if (newFilters.size() > 0) {// there are filters to be spread
            masterWorkerPlan = (CollectProducer) addNewFilters(newFilters, masterWorkerPlan);
            plan.setMasterWorkerPlan(masterWorkerPlan);
        }
    }

    private DbIterator addNewFilters(
            HashMap<String, HashMap<String, HashSet<Predicate>>> newFilters,
            DbIterator root) {
        if (!(root instanceof Operator)) {
            if (root instanceof SeqScan) {
                SeqScan s = (SeqScan) root;
                TupleDesc td = s.getTupleDesc();
                String alias = s.getAlias();
                String tableName = s.getTableName();
                DbIterator child = s;
                if (alias == null)
                    alias = tableName;
                HashMap<String, HashSet<Predicate>> fieldPreds = newFilters
                        .get(alias);
                if (fieldPreds != null) {
                    for (Entry<String, HashSet<Predicate>> e : fieldPreds
                            .entrySet()) {
                        String field = e.getKey();
                        HashSet<Predicate> preds = e.getValue();
                        for (Predicate p : preds) {
                            Predicate newP = new Predicate(
                                    td.fieldNameToIndex(alias + "." + field),
                                    p.getOp(), p.getOperand());
                            Filter f = new Filter(newP, child);
                            child = f;
                        }
                    }
                }
                return child;
            } else
                return root;
        }

        DbIterator[] children = ((Operator) root).getChildren();

        if (root instanceof Join || root instanceof HashEquiJoin) {
            children[0] = addNewFilters(newFilters, children[0]);
            children[1] = addNewFilters(newFilters, children[1]);
        } else {
            children[0] = addNewFilters(newFilters, children[0]);
        }
        ((Operator) root).setChildren(children);
        return root;
    }

    private void getEquivalentJoinFields(DbIterator root,
            HashMap<String, HashSet<String>> eq,
            HashMap<String, HashSet<Predicate>> joinKeyFilter) {

        if (!(root instanceof Operator))
            return;

        DbIterator[] children = ((Operator) root).getChildren();

        if (root instanceof Join || root instanceof HashEquiJoin) {
            String f1 = null;
            String f2 = null;
            Predicate.Op op = null;

            if (root instanceof Join) {
                Join j = (Join) root;
                f1 = j.getJoinField1Name();
                f2 = j.getJoinField2Name();
                op = j.getJoinPredicate().getOperator();
                children = j.getChildren();

            } else if (root instanceof HashEquiJoin) {
                HashEquiJoin j = (HashEquiJoin) root;
                f1 = j.getJoinField1Name();
                f2 = j.getJoinField2Name();
                op = j.getJoinPredicate().getOperator();
                children = j.getChildren();
            }

            if (op == Predicate.Op.EQUALS) {
                HashSet<String> f11 = eq.get(f1);
                if (f11 == null) {
                    f11 = new HashSet<String>();
                    eq.put(f1, f11);
                }
                f11.add(f2);
                HashSet<String> f21 = eq.get(f2);
                if (f21 == null) {
                    f21 = new HashSet<String>();
                    eq.put(f2, f21);
                }
                f21.add(f1);
                f11.addAll(f21);
                f21.addAll(f11);
            }

            getEquivalentJoinFields(children[0], eq, joinKeyFilter);
            getEquivalentJoinFields(children[1], eq, joinKeyFilter);
        } else {
            if (root instanceof Filter) {
                Filter f = (Filter) root;
                String fieldName = f.getTupleDesc().getFieldName(
                        f.getPredicate().getField());
                HashSet<Predicate> predicates = joinKeyFilter.get(fieldName);
                if (predicates == null) {
                    predicates = new HashSet<Predicate>();
                    joinKeyFilter.put(fieldName, predicates);
                }
                predicates.add(f.getPredicate());
            }
            getEquivalentJoinFields(children[0], eq, joinKeyFilter);
        }
    }

}
