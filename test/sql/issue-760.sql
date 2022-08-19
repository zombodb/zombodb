select pk_data
from consolidated_record_view
where zdb==>'(( #expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>(var_text_1 = "yellow" OR var_text_1 = "orange") ) AND #filter(var_text_filter = A)';

-- results should be pk_data [1, 4, 8]
