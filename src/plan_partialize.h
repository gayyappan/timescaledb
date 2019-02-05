
#include <postgres.h>
#include <optimizer/planner.h>

void		plan_add_partialize(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel);
