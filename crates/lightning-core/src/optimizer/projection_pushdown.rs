use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::{HashMap, HashSet};

pub struct ProjectionPushDown;

impl Default for ProjectionPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        Self
    }

    fn extract_property_indices(
        expr: &BoundExpression,
        indices: &mut HashMap<String, HashSet<usize>>,
    ) {
        match expr {
            BoundExpression::PropertyLookup(name, idx, _) => {
                indices.entry(name.clone()).or_default().insert(*idx);
            }
            BoundExpression::Variable(name, _) => {
                indices.entry(name.clone()).or_default().insert(0);
            }
            BoundExpression::Comparison(l, _, r) => {
                Self::extract_property_indices(l, indices);
                Self::extract_property_indices(r, indices);
            }
            BoundExpression::Arithmetic(l, _, r) => {
                Self::extract_property_indices(l, indices);
                Self::extract_property_indices(r, indices);
            }
            BoundExpression::Logical(l, _, r) => {
                Self::extract_property_indices(l, indices);
                Self::extract_property_indices(r, indices);
            }
            BoundExpression::Function(_, args, _) => {
                for arg in args {
                    Self::extract_property_indices(arg, indices);
                }
            }
            BoundExpression::List(exprs, _) => {
                for e in exprs {
                    Self::extract_property_indices(e, indices);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    Self::extract_property_indices(e, indices);
                }
                for (w, t) in when_then {
                    Self::extract_property_indices(w, indices);
                    Self::extract_property_indices(t, indices);
                }
                if let Some(e) = else_expression {
                    Self::extract_property_indices(e, indices);
                }
            }
            BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    Self::extract_property_indices(arg, indices);
                }
            }
            BoundExpression::Lambda(_, body) => {
                Self::extract_property_indices(body, indices);
            }
            BoundExpression::Parameter(_)
            | BoundExpression::Literal(_)
            | BoundExpression::NextVal(_) => {}
            _ => {}
        }
    }

    fn remap_expression_indices(
        expr: &mut BoundExpression,
        required_indices: &HashMap<String, HashSet<usize>>,
    ) {
        match expr {
            BoundExpression::PropertyLookup(var, idx, _) => {
                if let Some(set) = required_indices.get(var) {
                    let mut v: Vec<_> = set.iter().cloned().collect();
                    v.sort();
                    if let Some(pos) = v.iter().position(|&i| i == *idx) {
                        *idx = pos;
                    }
                }
            }
            BoundExpression::Comparison(l, _, r) => {
                Self::remap_expression_indices(l, required_indices);
                Self::remap_expression_indices(r, required_indices);
            }
            BoundExpression::Arithmetic(l, _, r) => {
                Self::remap_expression_indices(l, required_indices);
                Self::remap_expression_indices(r, required_indices);
            }
            BoundExpression::Logical(l, _, r) => {
                Self::remap_expression_indices(l, required_indices);
                Self::remap_expression_indices(r, required_indices);
            }
            BoundExpression::Function(_, args, _) => {
                for arg in args {
                    Self::remap_expression_indices(arg, required_indices);
                }
            }
            BoundExpression::List(exprs, _) => {
                for e in exprs {
                    Self::remap_expression_indices(e, required_indices);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    Self::remap_expression_indices(e, required_indices);
                }
                for (w, t) in when_then {
                    Self::remap_expression_indices(w, required_indices);
                    Self::remap_expression_indices(t, required_indices);
                }
                if let Some(e) = else_expression {
                    Self::remap_expression_indices(e, required_indices);
                }
            }
            BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    Self::remap_expression_indices(arg, required_indices);
                }
            }
            BoundExpression::Lambda(_, body) => {
                Self::remap_expression_indices(body, required_indices);
            }
            _ => {}
        }
    }

    fn push_down(
        &self,
        plan: LogicalOperator,
        required_indices: HashMap<String, HashSet<usize>>,
    ) -> Result<(LogicalOperator, HashMap<String, HashSet<usize>>)> {
        match plan {
            LogicalOperator::Projection(child, items) => {
                let mut my_indices = HashMap::new();
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Projection(Box::new(new_child), items),
                    child_indices,
                ))
            }
            LogicalOperator::Filter(child, cond) => {
                let mut my_indices = required_indices;
                Self::extract_property_indices(&cond, &mut my_indices);
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Filter(Box::new(new_child), cond),
                    child_indices,
                ))
            }
            LogicalOperator::Join(left, right, cond) => {
                let mut my_indices = required_indices;
                Self::extract_property_indices(&cond, &mut my_indices);
                let (new_left, left_indices) = self.push_down(*left, my_indices.clone())?;
                let (new_right, right_indices) = self.push_down(*right, my_indices.clone())?;
                let mut combined = left_indices;
                for (k, v) in right_indices {
                    combined.entry(k).or_default().extend(v);
                }
                Ok((
                    LogicalOperator::Join(Box::new(new_left), Box::new(new_right), cond),
                    combined,
                ))
            }
            LogicalOperator::Scan(table, var, mask, _, filter) => {
                let mut v = Vec::new();
                if let Some(set) = required_indices.get(&var) {
                    v = set.iter().cloned().collect();
                    v.sort();
                }
                Ok((
                    LogicalOperator::Scan(
                        table,
                        var,
                        mask,
                        if v.is_empty() { None } else { Some(v) },
                        filter,
                    ),
                    required_indices,
                ))
            }
            LogicalOperator::IndexScan(table, var, pk_name, pk_val, _) => {
                let mut v = Vec::new();
                if let Some(set) = required_indices.get(&var) {
                    v = set.iter().cloned().collect();
                    v.sort();
                }
                Ok((
                    LogicalOperator::IndexScan(
                        table,
                        var,
                        pk_name,
                        pk_val,
                        if v.is_empty() { None } else { Some(v) },
                    ),
                    required_indices,
                ))
            }
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            } => {
                let (new_child, child_indices) = self.push_down(*child, required_indices)?;
                Ok((
                    LogicalOperator::Aggregate {
                        child: Box::new(new_child),
                        group_by_cols,
                        dependent_group_by_cols,
                        aggregates,
                    },
                    child_indices,
                ))
            }
            LogicalOperator::Sort(child, items) => {
                let mut my_indices = required_indices;
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Sort(Box::new(new_child), items),
                    child_indices,
                ))
            }
            LogicalOperator::TopK(child, items, limit) => {
                let mut my_indices = required_indices;
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::TopK(Box::new(new_child), items, limit),
                    child_indices,
                ))
            }
            LogicalOperator::Limit(child, limit) => {
                let (new_child, child_indices) = self.push_down(*child, required_indices)?;
                Ok((
                    LogicalOperator::Limit(Box::new(new_child), limit),
                    child_indices,
                ))
            }
            LogicalOperator::Skip(child, skip) => {
                let (new_child, child_indices) = self.push_down(*child, required_indices)?;
                Ok((
                    LogicalOperator::Skip(Box::new(new_child), skip),
                    child_indices,
                ))
            }
            LogicalOperator::Set(child, assignments) => {
                let mut my_indices = required_indices;
                for assignment in &assignments {
                    my_indices
                        .entry(assignment.variable.clone())
                        .or_default()
                        .insert(0); // Always need _id for SET
                    Self::extract_property_indices(&assignment.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Set(Box::new(new_child), assignments),
                    child_indices,
                ))
            }
            LogicalOperator::Delete(child, vars, detach) => {
                let mut my_indices = required_indices;
                for (var, _) in &vars {
                    my_indices.entry(var.clone()).or_default().insert(0); // Always need _id for DELETE
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Delete(Box::new(new_child), vars, detach),
                    child_indices,
                ))
            }
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => {
                let mut my_indices = required_indices;
                my_indices
                    .entry(pattern.variable.clone().unwrap_or_default())
                    .or_default()
                    .insert(0);
                for assign in &on_create_assignments {
                    Self::extract_property_indices(&assign.expression, &mut my_indices);
                }
                for assign in &on_match_assignments {
                    Self::extract_property_indices(&assign.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Ok((
                    LogicalOperator::Merge {
                        child: Box::new(new_child),
                        pattern,
                        on_create_assignments,
                        on_match_assignments,
                    },
                    child_indices,
                ))
            }
            _ => {
                if let Some(child) = plan.get_child().cloned() {
                    let (new_child, child_indices) = self.push_down(child, required_indices)?;
                    let mut new_plan = plan.clone();
                    new_plan.set_child(new_child);
                    Ok((new_plan, child_indices))
                } else {
                    Ok((plan, required_indices))
                }
            }
        }
    }
}

impl Rule for ProjectionPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let required_indices = HashMap::new();
        let (optimized_plan, _) = self.push_down(plan, required_indices)?;
        Ok(optimized_plan)
    }
}
