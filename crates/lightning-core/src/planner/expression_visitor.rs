use crate::planner::binder::BoundExpression;

pub trait ExpressionVisitor {
    fn visit(&mut self, expr: &BoundExpression) {
        match expr {
            BoundExpression::Logical(l, _, r) => {
                self.visit(l);
                self.visit(r);
            }
            BoundExpression::Comparison(l, _, r) => {
                self.visit(l);
                self.visit(r);
            }
            BoundExpression::Arithmetic(l, _, r) => {
                self.visit(l);
                self.visit(r);
            }
            BoundExpression::Function(_, args, _) => {
                for arg in args {
                    self.visit(arg);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(i) = expression {
                    self.visit(i);
                }
                for (w, t) in when_then {
                    self.visit(w);
                    self.visit(t);
                }
                if let Some(e) = else_expression {
                    self.visit(e);
                }
            }
            BoundExpression::List(exprs, _) => {
                for expr in exprs {
                    self.visit(expr);
                }
            }
            BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    self.visit(arg);
                }
            }
            BoundExpression::Lambda(_, body) => {
                self.visit(body);
            }
            BoundExpression::Not(expr) => {
                self.visit(expr);
            }
            BoundExpression::Map(entries, _) => {
                for (_, expr) in entries {
                    self.visit(expr);
                }
            }
            BoundExpression::Exists(steps) | BoundExpression::CountSubquery(steps) => {
                for (match_clause, where_clause) in steps {
                    for element in &match_clause.elements {
                        if let crate::planner::binder::BoundMatchElement::Node(_, _, props) = element {
                            for (_, expr) in props {
                                self.visit(expr);
                            }
                        }
                    }
                    if let Some(w) = where_clause {
                        self.visit(&w.expression);
                    }
                }
            }
            _ => self.visit_leaf(expr),
        }
    }

    fn visit_leaf(&mut self, _expr: &BoundExpression) {}
}

pub trait ExpressionRewriter {
    fn rewrite(&mut self, expr: BoundExpression) -> BoundExpression {
        match expr {
            BoundExpression::Logical(l, op, r) => {
                BoundExpression::Logical(Box::new(self.rewrite(*l)), op, Box::new(self.rewrite(*r)))
            }
            BoundExpression::Comparison(l, op, r) => BoundExpression::Comparison(
                Box::new(self.rewrite(*l)),
                op,
                Box::new(self.rewrite(*r)),
            ),
            BoundExpression::Arithmetic(l, op, r) => BoundExpression::Arithmetic(
                Box::new(self.rewrite(*l)),
                op,
                Box::new(self.rewrite(*r)),
            ),
            BoundExpression::Function(name, args, ty) => {
                let mut rewritten_args = Vec::new();
                for arg in args {
                    rewritten_args.push(self.rewrite(arg));
                }
                BoundExpression::Function(name, rewritten_args, ty)
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                return_type,
            } => {
                let rewritten_input = expression.map(|i| Box::new(self.rewrite(*i)));
                let mut rewritten_when_thens = Vec::new();
                for (w, t) in when_then {
                    rewritten_when_thens.push((self.rewrite(w), self.rewrite(t)));
                }
                let rewritten_else = else_expression.map(|e| Box::new(self.rewrite(*e)));
                BoundExpression::Case {
                    expression: rewritten_input,
                    when_then: rewritten_when_thens,
                    else_expression: rewritten_else,
                    return_type,
                }
            }
            BoundExpression::List(exprs, ty) => {
                let mut rewritten_exprs = Vec::new();
                for expr in exprs {
                    rewritten_exprs.push(self.rewrite(expr));
                }
                BoundExpression::List(rewritten_exprs, ty)
            }
            BoundExpression::Aggregate(name, args, table_name) => {
                let mut rewritten_args = Vec::new();
                for arg in args {
                    rewritten_args.push(self.rewrite(arg));
                }
                BoundExpression::Aggregate(name, rewritten_args, table_name)
            }
            BoundExpression::Lambda(var, body) => {
                BoundExpression::Lambda(var, Box::new(self.rewrite(*body)))
            }
            BoundExpression::Not(expr) => {
                BoundExpression::Not(Box::new(self.rewrite(*expr)))
            }
            BoundExpression::Map(entries, ty) => {
                let mut rewritten = Vec::new();
                for (key, expr) in entries {
                    rewritten.push((key, self.rewrite(expr)));
                }
                BoundExpression::Map(rewritten, ty)
            }
            BoundExpression::Exists(steps) => {
                BoundExpression::Exists(steps) // subqueries handled at planner level
            }
            BoundExpression::CountSubquery(steps) => {
                BoundExpression::CountSubquery(steps)
            }
            _ => self.rewrite_leaf(expr),
        }
    }

    fn rewrite_leaf(&mut self, expr: BoundExpression) -> BoundExpression {
        expr
    }
}
