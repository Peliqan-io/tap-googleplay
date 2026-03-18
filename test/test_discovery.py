"""
Tests for discovery mode in tap-googleplay.

Tests the discover() and load_schemas() functions.
"""

import json
import pytest

from tap_googleplay import discover, load_schemas, Context, get_bookmark


class TestLoadSchemas:
    """Tests for load_schemas() function."""

    def test_returns_dict(self):
        """load_schemas returns a dictionary."""
        schemas = load_schemas()
        assert isinstance(schemas, dict)

    def test_contains_installs_schema(self):
        """load_schemas includes the 'installs' schema."""
        schemas = load_schemas()
        assert 'installs' in schemas

    def test_installs_schema_has_type(self):
        """installs schema has 'type' field."""
        schemas = load_schemas()
        assert 'type' in schemas['installs']

    def test_installs_schema_has_properties(self):
        """installs schema has 'properties' field."""
        schemas = load_schemas()
        assert 'properties' in schemas['installs']

    def test_installs_schema_required_fields(self):
        """installs schema contains required fields."""
        schemas = load_schemas()
        props = schemas['installs']['properties']
        
        required_fields = [
            'date',
            'package_name',
            'dimension_name',
            'dimension_value',
            'active_device_installs',
            'daily_device_installs',
            'daily_device_uninstalls',
            'daily_device_upgrades',
            'total_user_installs',
            'daily_user_installs',
            'daily_user_uninstalls',
            'install_events',
            'update_events',
            'uninstall_events',
        ]
        
        for field in required_fields:
            assert field in props, f"Missing required field: {field}"


class TestDiscover:
    """Tests for discover() function."""

    def test_returns_catalog_dict(self):
        """discover() returns a dictionary with 'streams' key."""
        catalog = discover()
        assert isinstance(catalog, dict)
        assert 'streams' in catalog

    def test_streams_is_list(self):
        """streams is a list."""
        catalog = discover()
        assert isinstance(catalog['streams'], list)

    def test_has_installs_stream(self):
        """catalog contains installs stream."""
        catalog = discover()
        stream_names = [s['stream'] for s in catalog['streams']]
        assert 'installs' in stream_names

    def test_installs_stream_structure(self):
        """installs stream has correct structure."""
        catalog = discover()
        installs = next(s for s in catalog['streams'] if s['stream'] == 'installs')
        
        assert 'stream' in installs
        assert 'tap_stream_id' in installs
        assert 'schema' in installs
        assert 'key_properties' in installs
        assert 'metadata' in installs

    def test_installs_tap_stream_id_matches_stream(self):
        """tap_stream_id matches stream name."""
        catalog = discover()
        installs = next(s for s in catalog['streams'] if s['stream'] == 'installs')
        
        assert installs['tap_stream_id'] == 'installs'

    def test_installs_key_properties(self):
        """installs stream has correct key_properties."""
        catalog = discover()
        installs = next(s for s in catalog['streams'] if s['stream'] == 'installs')
        
        expected_keys = ['date', 'package_name', 'dimension_name', 'dimension_value']
        assert installs['key_properties'] == expected_keys

    def test_installs_metadata_is_list(self):
        """metadata is a list."""
        catalog = discover()
        installs = next(s for s in catalog['streams'] if s['stream'] == 'installs')
        
        assert isinstance(installs['metadata'], list)

    def test_catalog_is_valid_json(self):
        """catalog can be serialized to valid JSON."""
        catalog = discover()
        json_str = json.dumps(catalog)
        
        # Should be able to parse it back
        parsed = json.loads(json_str)
        assert parsed == catalog


class TestGetBookmark:
    """Tests for get_bookmark() function."""

    def test_returns_state_bookmark_if_present(self, sample_state):
        """get_bookmark returns bookmark from state if present."""
        Context.state = sample_state
        Context.config = {'start_date': '2020-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('installs')
        assert bookmark == '2024-06-01T00:00:00Z'

    def test_returns_config_start_date_if_no_state(self):
        """get_bookmark returns config start_date if no bookmark in state."""
        Context.state = {}
        Context.config = {'start_date': '2024-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('installs')
        assert bookmark == '2024-01-01T00:00:00Z'

    def test_returns_config_start_date_if_different_stream(self, sample_state):
        """get_bookmark returns config start_date for stream not in state."""
        Context.state = sample_state
        Context.config = {'start_date': '2024-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('other_stream')
        assert bookmark == '2024-01-01T00:00:00Z'

    def test_returns_config_start_date_if_state_is_none(self):
        """get_bookmark handles None state."""
        Context.state = {'bookmarks': {}}
        Context.config = {'start_date': '2024-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('installs')
        assert bookmark == '2024-01-01T00:00:00Z'


class TestContextClass:
    """Tests for Context class methods."""

    def test_get_catalog_entry_returns_stream(self, sample_catalog):
        """get_catalog_entry returns the correct stream."""
        Context.catalog = sample_catalog
        Context.stream_map = {}  # Reset cache
        
        entry = Context.get_catalog_entry('installs')
        
        assert entry is not None
        assert entry['stream'] == 'installs'

    def test_get_catalog_entry_returns_none_for_missing(self, sample_catalog):
        """get_catalog_entry returns None for missing stream."""
        Context.catalog = sample_catalog
        Context.stream_map = {}
        
        entry = Context.get_catalog_entry('nonexistent')
        
        assert entry is None

    def test_get_schema_returns_schema_dict(self, sample_catalog):
        """get_schema returns the schema dictionary."""
        Context.catalog = sample_catalog
        
        schema = Context.get_schema('installs')
        
        assert isinstance(schema, dict)
        assert 'type' in schema
        assert 'properties' in schema

    def test_is_selected_with_metadata(self):
        """is_selected returns correct value based on metadata."""
        # Create catalog with selected stream
        catalog = {
            'streams': [{
                'stream': 'installs',
                'tap_stream_id': 'installs',
                'schema': {},
                'key_properties': [],
                'metadata': [
                    {'breadcrumb': [], 'metadata': {'selected': True}}
                ]
            }]
        }
        Context.catalog = catalog
        Context.stream_map = {}
        
        assert Context.is_selected('installs') is True

    def test_is_selected_returns_falsy_for_missing_stream(self, sample_catalog):
        """is_selected returns falsy value for nonexistent stream."""
        Context.catalog = sample_catalog
        Context.stream_map = {}
        
        result = Context.is_selected('nonexistent')
        
        # Returns None when stream not found (from get_catalog_entry returning None)
        assert not result
