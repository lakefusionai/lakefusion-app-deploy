"""update survivorship rules strategyRule format

Revision ID: 8cd7011307b7
Revises: 6ad578c4345f
Create Date: 2025-10-09 10:30:56.668065

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import json


# revision identifiers, used by Alembic.
revision: str = '8cd7011307b7'
down_revision: Union[str, None] = '6ad578c4345f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Update strategyRule in survivorshiprule table from old format to new format.
    Old: strategyRule: "[{"dataset_id": 40, "attribute_name": "date_updated"}]"
    New: strategyRule: "date_updated"
    """
    # Create a connection
    connection = op.get_bind()
    
    # Fetch all records from survivorshiprule table
    result = connection.execute(
        sa.text("SELECT entity_id, survivorship_id, rules FROM survivorshiprule WHERE is_active = true")
    )
    
    updates = []
    
    for row in result:
        entity_id = row[0]
        survivorship_id = row[1]
        rules = row[2]
        
        # Skip if rules is None or empty
        if not rules:
            continue
        
        # Parse the JSON rules
        try:
            if isinstance(rules, str):
                rules_list = json.loads(rules)
            else:
                rules_list = rules
                
            updated = False
            
            # Process each rule
            for rule in rules_list:
                if 'strategyRule' in rule and rule['strategyRule']:
                    strategy_rule = rule['strategyRule']
                    
                    # Check if strategyRule is a string that looks like old format
                    if isinstance(strategy_rule, str):
                        try:
                            # Try to parse it as JSON
                            parsed_rule = json.loads(strategy_rule)
                            
                            # Check if it's the old format (list of dicts with dataset_id and attribute_name)
                            if isinstance(parsed_rule, list) and len(parsed_rule) > 0:
                                if isinstance(parsed_rule[0], dict) and 'attribute_name' in parsed_rule[0]:
                                    # Extract the attribute_name from the first item
                                    attribute_name = parsed_rule[0]['attribute_name']
                                    if attribute_name:
                                        rule['strategyRule'] = attribute_name
                                        updated = True
                        except (json.JSONDecodeError, ValueError, TypeError):
                            # If it's not valid JSON, it might already be in new format
                            pass
            
            # If any rules were updated, add to update list
            if updated:
                updates.append({
                    'entity_id': entity_id,
                    'survivorship_id': survivorship_id,
                    'rules': json.dumps(rules_list)
                })
        
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Error processing survivorship rule for entity_id={entity_id}, survivorship_id={survivorship_id}: {e}")
            continue
    
    # Perform batch updates
    if updates:
        print(f"Updating {len(updates)} survivorship rules...")
        for update in updates:
            connection.execute(
                sa.text("""
                    UPDATE survivorshiprule 
                    SET rules = :rules
                    WHERE entity_id = :entity_id 
                    AND survivorship_id = :survivorship_id
                """),
                {
                    'rules': update['rules'],
                    'entity_id': update['entity_id'],
                    'survivorship_id': update['survivorship_id']
                }
            )
        print(f"Successfully updated {len(updates)} survivorship rules")
    else:
        print("No survivorship rules needed updating")


def downgrade() -> None:
    """
    Revert strategyRule back to old format.
    Note: This is a best-effort downgrade and may not be perfect if data has changed.
    """
    connection = op.get_bind()
    
    result = connection.execute(
        sa.text("SELECT entity_id, survivorship_id, rules FROM survivorshiprule WHERE is_active = true")
    )
    
    updates = []
    
    for row in result:
        entity_id = row[0]
        survivorship_id = row[1]
        rules = row[2]
        
        if not rules:
            continue
        
        try:
            if isinstance(rules, str):
                rules_list = json.loads(rules)
            else:
                rules_list = rules
                
            updated = False
            
            for rule in rules_list:
                if 'strategyRule' in rule and rule['strategyRule']:
                    strategy_rule = rule['strategyRule']
                    
                    # If it's a simple string (new format), convert back to old format
                    if isinstance(strategy_rule, str) and not strategy_rule.startswith('['):
                        # Convert to old format
                        old_format = json.dumps([{
                            "dataset_id": None,  # We can't recover the original dataset_id
                            "attribute_name": strategy_rule
                        }])
                        rule['strategyRule'] = old_format
                        updated = True
            
            if updated:
                updates.append({
                    'entity_id': entity_id,
                    'survivorship_id': survivorship_id,
                    'rules': json.dumps(rules_list)
                })
        
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Error reverting survivorship rule for entity_id={entity_id}, survivorship_id={survivorship_id}: {e}")
            continue
    
    if updates:
        print(f"Reverting {len(updates)} survivorship rules...")
        for update in updates:
            connection.execute(
                sa.text("""
                    UPDATE survivorshiprule 
                    SET rules = :rules
                    WHERE entity_id = :entity_id 
                    AND survivorship_id = :survivorship_id
                """),
                {
                    'rules': update['rules'],
                    'entity_id': update['entity_id'],
                    'survivorship_id': update['survivorship_id']
                }
            )
        print(f"Successfully reverted {len(updates)} survivorship rules")
    else:
        print("No survivorship rules needed reverting")