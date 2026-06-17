use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::{HashMap, HashSet};

/// Tracks column index requirements, preserving left/right side distinction
/// for Join output schemas where right-side columns are offset by the number
/// of left columns.
#[derive(Debug, Clone, Default)]
struct ColumnUsage {
    /// Per-variable required column indices in the operator's output space.
    indices: HashMap<String, HashSet<usize>>,
    /// Number of columns from the left side (0 for non-Join operators).
    left_col_count: usize,
    /// Variables that come from the right side of a Join.
    right_vars: HashSet<String>,
}

impl ColumnUsage {
    fn get(&self, var: &str) -> Option<&HashSet<usize>> {
        self.indices.get(var)
    }

    fn is_right_var(&self, var: &str) -> bool {
        self.right_vars.contains(var)
    }

    fn left_col_count(&self) -> usize {
        self.left_col_count
    }

    fn from_single(indices: HashMap<String, HashSet<usize>>) -> Self {
        Self {
            indices,
            left_col_count: 0,
            right_vars: HashSet::new(),
        }
    }
}

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
            BoundExpression::Not(inner) => Self::extract_property_indices(inner, indices),
            BoundExpression::Exists(steps) => {
                for (m, w) in steps {
                    for el in &m.elements {
                        if let crate::planner::binder::BoundMatchElement::Node(_, var, props) = el {
                            indices.entry(var.clone()).or_default().insert(0);
                            for (idx, _) in props {
                                indices.entry(var.clone()).or_default().insert(*idx);
                            }
                        }
                    }
                    if let Some(w) = w {
                        Self::extract_property_indices(&w.expression, indices);
                    }
                }
            }
            BoundExpression::CountSubquery(steps) => {
                for (m, w) in steps {
                    for el in &m.elements {
                        if let crate::planner::binder::BoundMatchElement::Node(_, var, props) = el {
                            indices.entry(var.clone()).or_default().insert(0);
                            for (idx, _) in props {
                                indices.entry(var.clone()).or_default().insert(*idx);
                            }
                        }
                    }
                    if let Some(w) = w {
                        Self::extract_property_indices(&w.expression, indices);
                    }
                }
            }
            BoundExpression::Map(entries, _) => {
                for (_, e) in entries {
                    Self::extract_property_indices(e, indices);
                }
            }
            BoundExpression::Parameter(_)
            | BoundExpression::Literal(_)
            | BoundExpression::NextVal(_) => {}
        }
    }

    fn remap_expression_indices(
        expr: &mut BoundExpression,
        column_usage: &ColumnUsage,
    ) {
        match expr {
            BoundExpression::PropertyLookup(var, idx, _) => {
                if let Some(set) = column_usage.get(var) {
                    let mut v: Vec<_> = set.iter().cloned().collect();
                    v.sort();
                    if let Some(pos) = v.iter().position(|&i| i == *idx) {
                        if column_usage.is_right_var(var) {
                            *idx = column_usage.left_col_count() + pos;
                        } else {
                            *idx = pos;
                        }
                    }
                }
            }
            BoundExpression::Comparison(l, _, r) => {
                Self::remap_expression_indices(l, column_usage);
                Self::remap_expression_indices(r, column_usage);
            }
            BoundExpression::Arithmetic(l, _, r) => {
                Self::remap_expression_indices(l, column_usage);
                Self::remap_expression_indices(r, column_usage);
            }
            BoundExpression::Logical(l, _, r) => {
                Self::remap_expression_indices(l, column_usage);
                Self::remap_expression_indices(r, column_usage);
            }
            BoundExpression::Function(_, args, _) => {
                for arg in args {
                    Self::remap_expression_indices(arg, column_usage);
                }
            }
            BoundExpression::List(exprs, _) => {
                for e in exprs {
                    Self::remap_expression_indices(e, column_usage);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    Self::remap_expression_indices(e, column_usage);
                }
                for (w, t) in when_then {
                    Self::remap_expression_indices(w, column_usage);
                    Self::remap_expression_indices(t, column_usage);
                }
                if let Some(e) = else_expression {
                    Self::remap_expression_indices(e, column_usage);
                }
            }
            BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    Self::remap_expression_indices(arg, column_usage);
                }
            }
            BoundExpression::Lambda(_, body) => {
                Self::remap_expression_indices(body, column_usage);
            }
            BoundExpression::Not(inner) => Self::remap_expression_indices(inner, column_usage),
            BoundExpression::Exists(steps) | BoundExpression::CountSubquery(steps) => {
                // Exists/CountSubquery expressions reference outer-scope variables
                // but their internal expressions are evaluated in a subquery scope
                // and do not need index remapping in the parent plan.
            }
            BoundExpression::Map(entries, _) => {
                for (_, e) in entries {
                    Self::remap_expression_indices(e, column_usage);
                }
            }
            _ => {}
        }
    }

    fn push_down(
        &self,
        plan: LogicalOperator,
        required_indices: HashMap<String, HashSet<usize>>,
    ) -> Result<(LogicalOperator, ColumnUsage)> {
        match plan {
            LogicalOperator::Projection(child, mut items) => {
                let mut my_indices = HashMap::new();
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                for item in &mut items {
                    Self::remap_expression_indices(&mut item.expression, &child_indices);
                }
                Ok((
                    LogicalOperator::Projection(Box::new(new_child), items),
                    child_indices,
                ))
            }
            LogicalOperator::Filter(child, mut cond) => {
                let mut my_indices = required_indices;
                Self::extract_property_indices(&cond, &mut my_indices);
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                Self::remap_expression_indices(&mut cond, &child_indices);
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

                // Count distinct left columns for offset calculation
                let all_left: HashSet<usize> = left_indices
                    .indices
                    .values()
                    .flat_map(|s| s.iter().cloned())
                    .collect();
                let left_col_count = all_left.iter().max().copied().unwrap_or(0).saturating_add(1);

                // Track which variables come from the right side
                let right_vars: HashSet<String> = right_indices.indices.keys().cloned().collect();

                // Merge indices: left indices stay as-is, right indices get their original values
                let mut combined_indices = left_indices.indices;
                for (k, v) in right_indices.indices {
                    combined_indices.entry(k).or_default().extend(v);
                }

                let combined = ColumnUsage {
                    indices: combined_indices,
                    left_col_count,
                    right_vars,
                };
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
                    ColumnUsage::from_single(required_indices),
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
                    ColumnUsage::from_single(required_indices),
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
            LogicalOperator::Sort(child, mut items) => {
                let mut my_indices = required_indices;
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                for item in &mut items {
                    Self::remap_expression_indices(&mut item.expression, &child_indices);
                }
                Ok((
                    LogicalOperator::Sort(Box::new(new_child), items),
                    child_indices,
                ))
            }
            LogicalOperator::TopK(child, mut items, limit) => {
                let mut my_indices = required_indices;
                for item in &items {
                    Self::extract_property_indices(&item.expression, &mut my_indices);
                }
                let (new_child, child_indices) = self.push_down(*child, my_indices)?;
                for item in &mut items {
                    Self::remap_expression_indices(&mut item.expression, &child_indices);
                }
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
                    Ok((plan, ColumnUsage::from_single(required_indices)))
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
