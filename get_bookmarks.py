import sys
sys.path.insert(0, '/Users/saydulloismatov/Documents/qlik sense api/qlik-sense-api')

from src.api.clients.qlik_engine import QlikEngineClient
from src.api.core.config import settings

def get_bookmarks():
    client = QlikEngineClient(settings)
    
    try:
        # Connect first
        client.connect()
        
        # Open the app
        app_id = "135812c5-7ba4-4d3e-a649-18e49ab5b1bb"  # afko app
        result = client.open_doc(app_id, no_data=False)
        app_handle = result['qReturn']['qHandle']
        
        # Get layout to access bookmark list
        layout = client.send_request('GetAllInfos', [], handle=app_handle)
        
        print("\nAll objects in afko app:")
        print("=" * 80)
        
        # Filter for bookmarks
        for item in layout.get('qInfos', []):
            if item.get('qType') == 'bookmark':
                bookmark_id = item.get('qId', 'N/A')
                
                # Get bookmark object for more details
                try:
                    bm_obj = client.send_request('GetBookmark', [bookmark_id], handle=app_handle)
                    bm_handle = bm_obj['qReturn']['qHandle']
                    
                    # Get properties
                    props = client.send_request('GetProperties', [], handle=bm_handle)
                    title = props.get('qMetaDef', {}).get('title', 'Untitled')
                    description = props.get('qMetaDef', {}).get('description', '')
                    
                    print(f"\nTitle: {title}")
                    print(f"ID: {bookmark_id}")
                    if description:
                        print(f"Description: {description}")
                    print("-" * 80)
                except Exception as e:
                    print(f"Could not get details for bookmark {bookmark_id}: {e}")
        
        client.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    get_bookmarks()
