package ch.ethz.infsec.monitor.visitor;

import ch.ethz.infsec.monitor.*;

public interface MformulaVisitor<T> {
    T visit(MPred f);
    T visit(MAnd f);
    T visit(MRel f);
    T visit(MExists f);
    T visit(MNext f);
    T visit(MOr f);
    T visit(MPrev f);
    T visit(MSince f);
    T visit(MUntil f);

}
