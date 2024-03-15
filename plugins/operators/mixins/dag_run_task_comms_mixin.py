class DagRunTaskCommsMixin:

    def ensure_task_comms_table_exists(self, conn):
        self.log.info("DagRunTaskCommsMixin.ensure_task_comms_table_exists called")

        sql = """
            CREATE TABLE IF NOT EXISTS transient_data.dag_run_task_comms (
              id SERIAL PRIMARY KEY,
              run_id CHAR(64),
              dag_id TEXT,
              task_id TEXT,
              variable_key TEXT,
              variable_value TEXT,
              updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
              createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT unique_variable_key UNIQUE (variable_key, task_id, run_id, dag_id)
            );

        """
        conn.execute(sql)

    def get_task_var(self, conn, context, variable_key):
        dag_id = context["dag_run"].dag_id
        run_id = context["run_id"]
        task_id = self.task_id

        self.log.info(f"DagRunTaskCommsMixin.get_task_var for {variable_key}")

        sql = """
        SELECT variable_value
        FROM transient_data.dag_run_task_comms
        WHERE dag_id=%s
        AND task_id=%s
        AND run_id=%s
        AND variable_key=%s
        """
        result = conn.execute(sql, (dag_id, task_id, run_id, variable_key)).fetchone()

        if result:
            self.log.info(f"DagRunTaskCommsMixin.get_task_var {variable_key} = {result[0]}")
            return result[0]  # Accessing by index as fetchone() returns a tuple
        else:
            self.log.info(f"DagRunTaskCommsMixin.get_task_var {variable_key} = None")
            return None

    def set_task_var(self, conn, context, variable_key, variable_value):
        dag_id = context["dag_run"].dag_id
        run_id = context["run_id"]
        task_id = self.task_id

        self.log.info(f"DagRunTaskCommsMixin.set_task_var for {variable_key} {variable_value}")
        sql = """
        INSERT INTO transient_data.dag_run_task_comms (dag_id, run_id, task_id, variable_key, variable_value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (variable_key, task_id, run_id, dag_id)
        DO UPDATE SET variable_value = EXCLUDED.variable_value,
                      updatedat = CURRENT_TIMESTAMP
        """
        conn.execute(sql, (dag_id, run_id, task_id, variable_key, variable_value))

    def clear_task_vars(self, conn, context):
        dag_id = context["dag_run"].dag_id
        run_id = context["run_id"]
        task_id = self.task_id

        self.log.info("DagRunTaskCommsMixin.clear_task_vars")
        sql = """
        DELETE FROM transient_data.dag_run_task_comms
        WHERE dag_id = %s
        AND task_id = %s
        AND run_id = %s
        """
        conn.execute(sql, (dag_id, run_id, task_id))
