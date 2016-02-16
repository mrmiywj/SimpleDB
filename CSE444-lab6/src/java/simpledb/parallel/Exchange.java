package simpledb.parallel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import simpledb.Operator;
import simpledb.TransactionId;

/**
 * The exchange operator, which will be used in implementing parallel simpledb.
 * 
 * */
public abstract class Exchange extends Operator {

    private static final long serialVersionUID = 1L;

    protected final ParallelOperatorID operatorID;

    public Exchange(ParallelOperatorID oID) {
        this.operatorID = oID;
    }

    public ParallelOperatorID getOperatorID() {
        return this.operatorID;
    }

    /**
     * Return the name of the exchange, used only to display the operator in the
     * operator tree
     * */
    public abstract String getName();

    /**
     * 
     * The identifier of exchange operators. In a query plan, there may be a set
     * of exchange operators, this ID class is used for the server and the
     * workers to find out which exchange operator is the owner of an arriving
     * ExchangeMessage.
     * 
     * */
    public static class ParallelOperatorID implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * The transaction, to which the operator belongs
         * */
        private TransactionId tId;

        /**
         * The id
         * */
        private int oId;

        private ParallelOperatorID(TransactionId tId, int oId) {
            this.tId = tId;
            this.oId = oId;
        }

        public boolean equals(Object o) {
            ParallelOperatorID oID = (ParallelOperatorID) o;
            return tId.equals(oID.tId) && oId == oID.oId;
        }

        public int hashCode() {
            return this.tId.hashCode() * 31 + this.oId;
        }

        public String toString() {
            return tId.toString() + "." + oId;
        }

        /**
         * The only way to create a ParallelOperatorID.
         * */
        public static ParallelOperatorID newID(TransactionId tId) {
            return new ParallelOperatorID(tId, idGenerator.getAndIncrement());
        }

        private static final AtomicInteger idGenerator = new AtomicInteger();
    }
}
