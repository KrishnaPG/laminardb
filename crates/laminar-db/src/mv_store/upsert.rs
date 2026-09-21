//! Keyed MV snapshots with quota-checked replacement and deletion deltas.

use super::admission::{size_overflow, MvLimits};
use super::weight_and_plain_cols;
use crate::error::DbError;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::ScalarValue;

/// Keyed running snapshot from a `__weight` changelog; stored rows omit the weight column.
pub(super) struct UpsertState {
    key_cols: Vec<usize>,
    key_converter: RowConverter,
    pub(super) rows: HashMap<OwnedRow, Vec<ScalarValue>>,
    pub(super) approx_bytes: usize,
}

pub(super) struct UpsertDelta {
    rows: HashMap<OwnedRow, Option<Vec<ScalarValue>>>,
    bytes: usize,
}

impl UpsertState {
    pub(super) fn new(schema: &SchemaRef, key_cols: &[usize]) -> Result<Self, DbError> {
        if key_cols
            .iter()
            .any(|&column| column >= schema.fields().len())
        {
            return Err(DbError::Storage(
                "upsert MV key column is outside its schema".into(),
            ));
        }
        let sort_fields: Vec<SortField> = key_cols
            .iter()
            .map(|&c| SortField::new(schema.field(c).data_type().clone()))
            .collect();
        let key_converter = RowConverter::new(sort_fields)
            .map_err(|e| DbError::Storage(format!("upsert MV key converter: {e}")))?;
        Ok(Self {
            key_cols: key_cols.to_vec(),
            key_converter,
            rows: HashMap::new(),
            approx_bytes: 0,
        })
    }

    fn keys(&self, batch: &RecordBatch) -> Result<arrow::row::Rows, DbError> {
        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&c| Arc::clone(batch.column(c)))
            .collect();
        self.key_converter
            .convert_columns(&key_arrays)
            .map_err(|e| DbError::Storage(format!("upsert MV key conversion: {e}")))
    }

    fn row_size(key: &OwnedRow, values: &Vec<ScalarValue>) -> Result<usize, DbError> {
        let spare = (values.capacity() - values.len())
            .checked_mul(std::mem::size_of::<ScalarValue>())
            .ok_or_else(size_overflow)?;
        let base = std::mem::size_of::<(OwnedRow, Vec<ScalarValue>)>()
            .checked_add(key.as_ref().len())
            .and_then(|bytes| bytes.checked_add(spare))
            .ok_or_else(size_overflow)?;
        values.iter().try_fold(base, |bytes, value| {
            bytes.checked_add(value.size()).ok_or_else(size_overflow)
        })
    }

    /// Stage `+weight` upserts and `-weight` deletes without touching live state.
    fn stage_batch(
        &self,
        batch: &RecordBatch,
        staged: &mut HashMap<OwnedRow, Option<Vec<ScalarValue>>>,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_cols) = weight_and_plain_cols(batch)?;
        let keys = self.keys(batch)?;

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "upsert MV weight is null at row {row_idx}"
                )));
            }
            let key = keys.row(row_idx).owned();
            let w = weights.value(row_idx);
            if w > 0 {
                let mut vals = Vec::with_capacity(plain_cols.len());
                for &c in &plain_cols {
                    vals.push(
                        ScalarValue::try_from_array(batch.column(c), row_idx)
                            .map_err(|e| DbError::Storage(format!("upsert MV scalar: {e}")))?,
                    );
                }
                staged.insert(key, Some(vals));
            } else if w < 0 {
                staged.insert(key, None);
            }
        }
        Ok(())
    }

    pub(super) fn prepare_cycle(
        &self,
        name: &str,
        batches: &[RecordBatch],
        limits: MvLimits,
    ) -> Result<UpsertDelta, DbError> {
        let mut staged = HashMap::new();
        for batch in batches {
            self.stage_batch(batch, &mut staged)?;
        }
        let (mut rows, mut removed, mut added) = (self.rows.len(), 0usize, 0usize);
        for (key, replacement) in &staged {
            if let Some(old) = self.rows.get(key) {
                removed = removed
                    .checked_add(Self::row_size(key, old)?)
                    .ok_or_else(size_overflow)?;
                rows -= 1;
            }
            if let Some(values) = replacement {
                added = added
                    .checked_add(Self::row_size(key, values)?)
                    .ok_or_else(size_overflow)?;
                rows = rows.checked_add(1).ok_or_else(size_overflow)?;
            }
        }
        let bytes = self
            .approx_bytes
            .checked_sub(removed)
            .and_then(|bytes| bytes.checked_add(added))
            .ok_or_else(size_overflow)?;
        limits.validate(name, rows, bytes)?;
        Ok(UpsertDelta {
            rows: staged,
            bytes,
        })
    }

    pub(super) fn apply_prepared(&mut self, delta: UpsertDelta) {
        for (key, replacement) in delta.rows {
            if let Some(values) = replacement {
                self.rows.insert(key, values);
            } else {
                self.rows.remove(&key);
            }
        }
        self.approx_bytes = delta.bytes;
    }

    /// Restore from a materialized plain snapshot (no weight column): every row is an insert.
    pub(super) fn load_snapshot(
        &mut self,
        name: &str,
        batch: &RecordBatch,
        limits: MvLimits,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = self.keys(batch)?;
        for row_idx in 0..batch.num_rows() {
            let key = keys.row(row_idx).owned();
            let mut vals = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                vals.push(
                    ScalarValue::try_from_array(batch.column(c), row_idx)
                        .map_err(|e| DbError::Storage(format!("upsert MV restore scalar: {e}")))?,
                );
            }
            let mut bytes = self.approx_bytes;
            if let Some(old) = self.rows.get(&key) {
                bytes = bytes
                    .checked_sub(Self::row_size(&key, old)?)
                    .ok_or_else(size_overflow)?;
            }
            bytes = bytes
                .checked_add(Self::row_size(&key, &vals)?)
                .ok_or_else(size_overflow)?;
            let rows = self.rows.len() + usize::from(!self.rows.contains_key(&key));
            limits.validate(name, rows, bytes)?;
            self.rows.insert(key, vals);
            self.approx_bytes = bytes;
        }
        Ok(())
    }

    pub(super) fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        // One pass over the row map assembling all columns, rather than one full map scan per column.
        let ncols = schema.fields().len();
        let mut columns: Vec<Vec<ScalarValue>> = (0..ncols)
            .map(|_| Vec::with_capacity(self.rows.len()))
            .collect();
        for row in self.rows.values() {
            for (c, col) in columns.iter_mut().enumerate() {
                col.push(row[c].clone());
            }
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for col in columns {
            arrays.push(
                ScalarValue::iter_to_array(col)
                    .map_err(|e| DbError::Storage(format!("upsert MV column build: {e}")))?,
            );
        }
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("upsert MV batch assembly: {e}")))
    }
}
