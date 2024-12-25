from airflow.models import DagBag

def test_dag_integrity():
    """
    Test the integrity of DAGs in the DagBag.
    This test performs the following checks:
    1. Ensures that there are no DAG naming convention or import errors.
    2. Verifies that each DAG has associated tags.
    3. Confirms that each DAG is paused upon creation.
    Assertions:
    - No import errors or naming convention issues in the DAGs.
    - Each DAG has at least one tag.
    - Each DAG is paused upon creation.
    Raises:
    - AssertionError: If any of the above conditions are not met.
    """
    
    # check if the DAGs are named according to the naming convention and import errors
    dagbag = DagBag(include_examples=False)
    assert not dagbag.import_errors, 'There are DAG naming convention or import errors.'
    # check if all the have a tag associated with them and are paused upon creation
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f'DAG {dag_id} is missing tags.'
        assert dag.is_paused_upon_creation, f'DAG {dag_id} is not paused upon creation.'
    