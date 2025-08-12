#!/usr/bin/env python3
"""
LLM Configuration and Integration Module
Supports multiple LLM providers for economic intelligence analysis
"""

import os
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import json

class LLMProvider(Enum):
    """Supported LLM providers"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE_OPENAI = "azure_openai"
    LOCAL_OLLAMA = "ollama"
    HUGGINGFACE = "huggingface"

@dataclass
class LLMConfig:
    """LLM configuration settings"""
    provider: LLMProvider
    model_name: str
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    max_tokens: int = 4000
    temperature: float = 0.1
    timeout: int = 30
    retry_attempts: int = 3
    
class LLMClient:
    """Universal LLM client for economic analysis"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the appropriate LLM client based on provider"""
        try:
            if self.config.provider == LLMProvider.OPENAI:
                self._init_openai_client()
            elif self.config.provider == LLMProvider.ANTHROPIC:
                self._init_anthropic_client()
            elif self.config.provider == LLMProvider.AZURE_OPENAI:
                self._init_azure_openai_client()
            elif self.config.provider == LLMProvider.LOCAL_OLLAMA:
                self._init_ollama_client()
            elif self.config.provider == LLMProvider.HUGGINGFACE:
                self._init_huggingface_client()
            else:
                raise ValueError(f"Unsupported LLM provider: {self.config.provider}")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM client: {e}")
            self.client = None
    
    def _init_openai_client(self):
        """Initialize OpenAI client"""
        try:
            import openai
            self.client = openai.OpenAI(
                api_key=self.config.api_key or os.getenv('OPENAI_API_KEY'),
                base_url=self.config.api_base
            )
            self.logger.info("OpenAI client initialized")
        except ImportError:
            self.logger.error("OpenAI package not installed. Run: pip install openai")
        except Exception as e:
            self.logger.error(f"OpenAI client initialization failed: {e}")
    
    def _init_anthropic_client(self):
        """Initialize Anthropic Claude client"""
        try:
            import anthropic
            self.client = anthropic.Anthropic(
                api_key=self.config.api_key or os.getenv('ANTHROPIC_API_KEY')
            )
            self.logger.info("Anthropic client initialized")
        except ImportError:
            self.logger.error("Anthropic package not installed. Run: pip install anthropic")
        except Exception as e:
            self.logger.error(f"Anthropic client initialization failed: {e}")
    
    def _init_azure_openai_client(self):
        """Initialize Azure OpenAI client"""
        try:
            import openai
            self.client = openai.AzureOpenAI(
                api_key=self.config.api_key or os.getenv('AZURE_OPENAI_API_KEY'),
                api_version=os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-01'),
                azure_endpoint=self.config.api_base or os.getenv('AZURE_OPENAI_ENDPOINT')
            )
            self.logger.info("Azure OpenAI client initialized")
        except ImportError:
            self.logger.error("OpenAI package not installed. Run: pip install openai")
        except Exception as e:
            self.logger.error(f"Azure OpenAI client initialization failed: {e}")
    
    def _init_ollama_client(self):
        """Initialize local Ollama client"""
        try:
            import ollama
            self.client = ollama.Client(
                host=self.config.api_base or 'http://localhost:11434'
            )
            self.logger.info("Ollama client initialized")
        except ImportError:
            self.logger.error("Ollama package not installed. Run: pip install ollama")
        except Exception as e:
            self.logger.error(f"Ollama client initialization failed: {e}")
    
    def _init_huggingface_client(self):
        """Initialize Hugging Face client"""
        try:
            from transformers import pipeline
            self.client = pipeline(
                "text-generation",
                model=self.config.model_name,
                token=self.config.api_key or os.getenv('HUGGINGFACE_API_KEY')
            )
            self.logger.info("Hugging Face client initialized")
        except ImportError:
            self.logger.error("Transformers package not installed. Run: pip install transformers")
        except Exception as e:
            self.logger.error(f"Hugging Face client initialization failed: {e}")
    
    def is_available(self) -> bool:
        """Check if LLM client is available"""
        return self.client is not None
    
    def generate_analysis(self, prompt: str, context_data: Dict[str, Any] = None, analysis_type: str = None) -> str:
        """Generate analysis using the configured LLM"""
        if not self.is_available():
            raise RuntimeError("LLM client is not available")
        
        try:
            # Enhanced prompt with analysis type if provided
            enhanced_prompt = prompt
            if analysis_type:
                enhanced_prompt = f"[Analysis Type: {analysis_type}]\n\n{prompt}"
            
            if self.config.provider == LLMProvider.OPENAI:
                return self._generate_openai_analysis(enhanced_prompt, context_data)
            elif self.config.provider == LLMProvider.ANTHROPIC:
                return self._generate_anthropic_analysis(enhanced_prompt, context_data)
            elif self.config.provider == LLMProvider.AZURE_OPENAI:
                return self._generate_azure_openai_analysis(enhanced_prompt, context_data)
            elif self.config.provider == LLMProvider.LOCAL_OLLAMA:
                return self._generate_ollama_analysis(enhanced_prompt, context_data)
            elif self.config.provider == LLMProvider.HUGGINGFACE:
                return self._generate_huggingface_analysis(enhanced_prompt, context_data)
            else:
                raise ValueError(f"Unsupported provider: {self.config.provider}")
                
        except Exception as e:
            self.logger.error(f"Analysis generation failed: {e}")
            raise
    
    def _generate_openai_analysis(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Generate analysis using OpenAI"""
        messages = [
            {
                "role": "system",
                "content": "You are an expert economic analyst specializing in Singapore's economy. Provide detailed, data-driven insights based on the provided information."
            },
            {
                "role": "user",
                "content": self._format_prompt_with_context(prompt, context_data)
            }
        ]
        
        response = self.client.chat.completions.create(
            model=self.config.model_name,
            messages=messages,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
            timeout=self.config.timeout
        )
        
        return response.choices[0].message.content
    
    def _generate_anthropic_analysis(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Generate analysis using Anthropic Claude"""
        formatted_prompt = self._format_prompt_with_context(prompt, context_data)
        
        response = self.client.messages.create(
            model=self.config.model_name,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
            messages=[
                {
                    "role": "user",
                    "content": formatted_prompt
                }
            ]
        )
        
        return response.content[0].text
    
    def _generate_azure_openai_analysis(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Generate analysis using Azure OpenAI"""
        return self._generate_openai_analysis(prompt, context_data)  # Same API
    
    def _generate_ollama_analysis(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Generate analysis using local Ollama"""
        formatted_prompt = self._format_prompt_with_context(prompt, context_data)
        
        response = self.client.generate(
            model=self.config.model_name,
            prompt=formatted_prompt,
            options={
                "temperature": self.config.temperature,
                "num_predict": self.config.max_tokens
            }
        )
        
        return response['response']
    
    def _generate_huggingface_analysis(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Generate analysis using Hugging Face"""
        formatted_prompt = self._format_prompt_with_context(prompt, context_data)
        
        response = self.client(
            formatted_prompt,
            max_length=self.config.max_tokens,
            temperature=self.config.temperature,
            do_sample=True
        )
        
        return response[0]['generated_text']
    
    def _format_prompt_with_context(self, prompt: str, context_data: Dict[str, Any] = None) -> str:
        """Format prompt with context data"""
        if context_data is None:
            return prompt
        
        context_str = "\n\nContext Data:\n"
        context_str += json.dumps(context_data, indent=2, default=str)
        
        return prompt + context_str

class EconomicAnalysisPrompts:
    """Pre-defined prompts for economic analysis"""
    
    @staticmethod
    def business_formation_analysis() -> str:
        return """
Analyze the business formation trends based on the provided ACRA company data. Focus on:

1. Industry distribution and emerging sectors
2. Registration patterns and seasonal trends
3. Business lifecycle indicators (active vs ceased companies)
4. Geographic concentration patterns
5. Data quality assessment and reliability

Provide specific insights about:
- Key growth sectors and their implications
- Potential economic indicators reflected in business formations
- Risk factors or concerning patterns
- Recommendations for policymakers

Format your response with clear sections and actionable insights.
"""
    
    @staticmethod
    def economic_indicators_analysis() -> str:
        return """
Analyze the economic indicators data from SingStat. Focus on:

1. GDP growth trends and sustainability
2. Inflation patterns and price stability
3. Employment indicators and labor market health
4. Cross-indicator relationships and correlations
5. International context and comparative analysis

Provide insights on:
- Economic momentum and direction
- Policy implications of current indicators
- Risk factors and early warning signals
- Forecasting considerations

Include confidence levels and data quality assessments.
"""
    
    @staticmethod
    def government_expenditure_analysis() -> str:
        return """
Analyze government expenditure patterns and their economic impact. Focus on:

1. Spending allocation across sectors
2. Development vs operational expenditure balance
3. Economic multiplier effects
4. Alignment with economic priorities
5. Fiscal sustainability indicators

Provide analysis on:
- Investment effectiveness and ROI
- Sector-specific impacts on business formation
- Policy coherence and strategic alignment
- Recommendations for optimization

Consider both direct and indirect economic effects.
"""
    
    @staticmethod
    def property_market_analysis() -> str:
        return """
Analyze property market trends and their broader economic implications. Focus on:

1. Rental price trends across districts and property types
2. Market liquidity and transaction volumes
3. Affordability indicators and accessibility
4. Commercial vs residential market dynamics
5. Regional development patterns

Provide insights on:
- Market health and stability indicators
- Economic growth correlation
- Policy intervention needs
- Investment climate implications

Include risk assessments and market outlook.
"""
    
    @staticmethod
    def anomaly_detection_prompt() -> str:
        return """
Analyze the provided economic data for anomalies, outliers, and unusual patterns. Focus on:

1. Statistical outliers in key metrics
2. Trend breaks and structural changes
3. Cross-dataset inconsistencies
4. Seasonal pattern deviations
5. Data quality issues

For each anomaly identified, provide:
- Severity assessment (low/medium/high/critical)
- Potential causes and explanations
- Impact on related economic indicators
- Recommended investigation steps
- Monitoring requirements

Prioritize anomalies by economic significance and urgency.
"""
    
    @staticmethod
    def forecasting_prompt() -> str:
        return """
Generate economic forecasts based on historical trends and current indicators. Focus on:

1. Short-term projections (3-6 months)
2. Medium-term outlook (6-18 months)
3. Key assumption identification
4. Scenario analysis (optimistic/base/pessimistic)
5. Confidence intervals and uncertainty quantification

Provide forecasts for:
- Business formation rates
- Economic growth indicators
- Market trends and cycles
- Policy impact projections

Include methodology explanation and risk factors.
"""

def get_default_config() -> LLMConfig:
    """Get default LLM configuration based on available providers"""
    
    # Default to local Ollama with Llama 8B for this implementation
    return LLMConfig(
        provider=LLMProvider.LOCAL_OLLAMA,
        model_name="llama3.1:8b",  # Using Llama 8B model for optimal performance
        api_base="http://localhost:11434",
        temperature=0.1,
        max_tokens=8000  # Increased for more comprehensive analysis and larger datasets
    )

def create_llm_client(config: LLMConfig = None) -> LLMClient:
    """Create and return an LLM client"""
    if config is None:
        config = get_default_config()
    
    return LLMClient(config)

# Example usage and testing
if __name__ == "__main__":
    print("🤖 LLM Configuration Test")
    print("=" * 30)
    
    # Test configuration
    config = get_default_config()
    print(f"Default provider: {config.provider.value}")
    print(f"Model: {config.model_name}")
    
    # Test client creation
    try:
        client = create_llm_client(config)
        print(f"Client available: {client.is_available()}")
        
        if client.is_available():
            print("\n✅ LLM client ready for economic analysis")
        else:
            print("\n⚠️  LLM client not available - check configuration")
            
    except Exception as e:
        print(f"\n❌ Error creating LLM client: {e}")
    
    print("\n📝 Available analysis prompts:")
    prompts = EconomicAnalysisPrompts()
    print("   - Business Formation Analysis")
    print("   - Economic Indicators Analysis")
    print("   - Government Expenditure Analysis")
    print("   - Property Market Analysis")
    print("   - Anomaly Detection")
    print("   - Economic Forecasting")