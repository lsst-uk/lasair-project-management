# how to make a query from selects, tables, and conditions. Plus pages. Plus time constraints.

def make_query(selected, tables, conditions, page, perpage, check_days_ago, days_ago_candidates, days_ago_objects):
# select some quantitites from some tables
    sqlquery_real  = 'SELECT /*+ MAX_EXECUTION_TIME(300000) */ ' 
    sqlquery_real += selected
    sqlquery_real += ' FROM ' + tables

# if they added a days_ago clause, compute it here and prepent to conditions
    toktables = [x.strip() for x in tables.split(',')]
    time_conditions = []
    if check_days_ago:
        if 'objects' in toktables:
            time_conditions.append('objects.jdmax > JDNOW() - %.5f' % days_ago_objects)
        if 'candidates' in toktables:
            time_conditions.append('candidates.jd > JDNOW() - %.5f' % days_ago_candidates)
    if len(conditions.strip()) > 0:
        new_conditions = ' AND '.join(time_conditions + [conditions])
    else:
        new_conditions = ' AND '.join(time_conditions)

# list of joining conditions is prepended
    join_list = []
    toktables = [x.strip() for x in tables.split(',')]
    if 'objects' in toktables:
        toktables.remove('objects')
        for table in toktables:
            if table.startswith('sherlock'):
                join_list.append('objects.primaryId = %s.transient_object_id' % table)
            else:
                join_list.append('objects.objectId = %s.objectId' % table)
    if len(new_conditions.strip()) > 0:
        join_new_conditions = ' AND '.join(join_list + [new_conditions])
    else:
        join_new_conditions = ' AND '.join(join_list)

# conditions may have no where clause just order by
    if join_new_conditions.strip().lower().startswith('order'):
        sqlquery_real += ' ' + join_new_conditions
    else:
# where clause and may also have order by included
        if len(join_new_conditions.strip()) > 0:
            sqlquery_real += ' WHERE ' + join_new_conditions

    sqlquery_real += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
    return sqlquery_real

def topic_name(name):
    return ''.join(e for e in name if (e.isalnum() and e.isascii()) or e=='_' or e=='-' or e=='.')

