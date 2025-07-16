#!/usr/bin/env python3
"""
Data Record Model for Economic Intelligence Platform
Defines the standard data structure for all processed records.
"""

from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class DataRecord(BaseModel):
    """Standard data record structure for all data sources"""
    
    source: str = Field(..., description="Data source name (e.g., 'ACRA', 'SingStat', 'URA')")
    timestamp: datetime = Field(..., description="Timestamp when the record was processed")
    data_type: str = Field(..., description="Type of data (e.g., 'company', 'economic_indicator', 'geospatial')")
    raw_data: Dict[str, Any] = Field(..., description="Original raw data from the source")
    processed_data: Dict[str, Any] = Field(..., description="Processed and standardized data")
    
    # Optional metadata fields
    record_id: Optional[str] = Field(None, description="Unique identifier for the record")
    quality_score: Optional[float] = Field(None, description="Data quality score (0.0 to 1.0)")
    validation_errors: Optional[list] = Field(None, description="List of validation errors if any")
    processing_notes: Optional[str] = Field(None, description="Additional processing notes")
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
    def to_kafka_message(self) -> Dict[str, Any]:
        """Convert to Kafka message format"""
        return self.model_dump()
    
    def validate_quality(self) -> float:
        """Calculate and return data quality score"""
        score = 1.0
        
        # Check for missing required fields
        if not self.raw_data:
            score -= 0.3
        
        if not self.processed_data:
            score -= 0.3
            
        # Check for validation errors
        if self.validation_errors:
            score -= 0.2 * len(self.validation_errors)
        
        # Ensure score is between 0 and 1
        score = max(0.0, min(1.0, score))
        
        # Update the quality score
        self.quality_score = score
        
        return score