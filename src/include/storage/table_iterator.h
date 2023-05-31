#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"

class TableHeap;

class TableIterator {
public:
  explicit TableIterator(BufferPoolManager *buffer_pool_manager, Schema *schema,RowId rowId);

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
    RowId row_;
    BufferPoolManager* buffer_;
    Schema* schema_;
    Row* row=nullptr;
};

#endif  // MINISQL_TABLE_ITERATOR_H
