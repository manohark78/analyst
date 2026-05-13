package com.enterprise.dataanalyst.planning;

import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.semantic.SchemaGrounder;
import com.enterprise.dataanalyst.state.AnalyticalContext;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Execution Planner — orchestrates the transformation of a user query into an executable plan.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionPlanner {

    private final SchemaGrounder schemaGrounder;
    private final SqlGenerator sqlGenerator;

    public String planSql(String query, List<DatasetInfo> datasets, AnalyticalContext context) {
        // 1. Ground the query to a specific table and columns from the workspace
        SchemaGrounder.GroundedSchema grounded = schemaGrounder.ground(query, datasets);

        // 2. Update context with active table if it changed
        if (grounded.getTable() != null) {
            context.setActiveTableName(grounded.getTable().getTableName());
        }

        // 3. Generate context-aware SQL
        return sqlGenerator.generate(query, grounded.getTable(), grounded.getColumns(), context);
    }
}
