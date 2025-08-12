from typing import List, Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context


class CollectFitResultsOperator(BaseOperator):
    """Operator to collect fit results from all column tasks"""
    
    def __init__(
        self,
        columns: List[str],
        **kwargs
    ):
        super().__init__(**kwargs)
        self.columns = columns
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """Collect and consolidate fit results from all column tasks"""
        self.log.info(f"Collecting fit results for columns: {self.columns}")
        
        collected_results = {}
        
        for column in self.columns:
            try:
                # Get fit results from corresponding fit task
                fit_results = context['task_instance'].xcom_pull(
                    key=f'fit_results_{column}', 
                    task_ids=f'fit_columns_parallel.fit_{column}'
                )
                
                if fit_results:
                    collected_results[column] = fit_results
                    self.log.info(f"Collected fit results for column: {column}")
                else:
                    self.log.warning(f"No fit results found for column: {column}")
                    
            except Exception as e:
                self.log.error(f"Error collecting fit results for column {column}: {str(e)}")
                raise
        
        if len(collected_results) != len(self.columns):
            missing_columns = set(self.columns) - set(collected_results.keys())
            raise ValueError(f"Missing fit results for columns: {missing_columns}")
        
        # Save consolidated results
        context['task_instance'].xcom_push(key='all_fit_results', value=collected_results)
        
        self.log.info(f"Successfully collected fit results for {len(collected_results)} columns")
        
        return {
            'status': 'success',
            'columns_fitted': list(collected_results.keys()),
            'total_columns': len(collected_results)
        }