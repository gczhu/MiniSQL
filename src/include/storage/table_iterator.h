#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(BufferPoolManager *buffer_pool_manager, Schema *schema,RowID rowId)
                        :row_(rowId),buffer_(buffer_pool_manager),schema_(schema){}

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
    RowID row_;
    BufferPoolManager* buffer_;
    Schema* schema_;
    Row* row=nullptr;
};

#endif  // MINISQL_TABLE_ITERATOR_H
