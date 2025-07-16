class LogicalPlan:
    pass

class TableScan(LogicalPlan):
    def __init__(self, table_name):
        self.table_name = table_name

class Projection(LogicalPlan):
    def __init__(self, columns, child):
        self.columns = columns
        self.child = child

class Selection(LogicalPlan):
    def __init__(self, predicate, child):
        self.predicate = predicate
        self.child = child

class Filter(LogicalPlan):
    def __init__(self, child, condition):
        self.child = child
        self.condition = condition

class Limit(LogicalPlan):
    def __init__(self, count, child):
        self.count = count
        self.child = child

class OrderBy(LogicalPlan):
    def __init__(self, column, direction, child):
        self.column = column
        self.direction = direction
        self.child = child