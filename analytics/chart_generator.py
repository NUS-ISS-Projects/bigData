#!/usr/bin/env python3
"""
Interactive Chart Generator for Economic Intelligence Dashboard
Creates Plotly charts from visual intelligence data
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class InteractiveChartGenerator:
    """Generate interactive Plotly charts from economic intelligence data"""
    
    def __init__(self):
        self.color_palette = {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e', 
            'success': '#2ca02c',
            'warning': '#d62728',
            'info': '#9467bd',
            'light': '#8c564b',
            'dark': '#e377c2'
        }
        
        self.chart_themes = {
            'default': 'plotly',
            'dark': 'plotly_dark',
            'white': 'plotly_white',
            'presentation': 'presentation'
        }
    
    def create_chart_from_data(self, chart_config: Dict[str, Any], theme: str = 'default') -> go.Figure:
        """Create a Plotly chart from chart configuration data"""
        try:
            chart_type = chart_config.get('type', 'bar')
            data = chart_config.get('data', {})
            title = chart_config.get('title', 'Chart')
            description = chart_config.get('description', '')
            
            # Route to appropriate chart creation method
            if chart_type == 'pie':
                return self._create_pie_chart(data, title, description, theme)
            elif chart_type == 'donut':
                return self._create_donut_chart(data, title, description, theme)
            elif chart_type == 'bar':
                return self._create_bar_chart(data, title, description, theme)
            elif chart_type == 'horizontal_bar':
                return self._create_horizontal_bar_chart(data, title, description, theme)
            elif chart_type == 'line':
                return self._create_line_chart(data, title, description, theme)
            elif chart_type == 'multi_line':
                return self._create_multi_line_chart(data, title, description, theme)
            elif chart_type == 'histogram':
                return self._create_histogram(data, title, description, theme)
            elif chart_type == 'scatter':
                return self._create_scatter_plot(data, title, description, theme)
            elif chart_type == 'radar':
                return self._create_radar_chart(data, title, description, theme)
            elif chart_type == 'gauge':
                return self._create_gauge_chart(data, title, description, theme)
            elif chart_type == 'speedometer':
                return self._create_speedometer_chart(data, title, description, theme)
            elif chart_type == 'metric_cards':
                return self._create_metric_cards(data, title, description, theme)
            elif chart_type == 'heatmap':
                return self._create_heatmap(data, title, description, theme)
            elif chart_type == 'treemap':
                return self._create_treemap(data, title, description, theme)
            elif chart_type == 'sunburst':
                return self._create_sunburst_chart(data, title, description, theme)
            else:
                logger.warning(f"Unknown chart type: {chart_type}, defaulting to bar chart")
                return self._create_bar_chart(data, title, description, theme)
                
        except Exception as e:
            logger.error(f"Error creating chart: {e}")
            return self._create_error_chart(str(e))
    
    def _create_pie_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a pie chart"""
        fig = px.pie(
            values=data.get('values', []),
            names=data.get('labels', []),
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Value: %{value}<br>Percentage: %{percent}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            showlegend=True,
            annotations=[dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_donut_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a donut chart"""
        fig = px.pie(
            values=data.get('values', []),
            names=data.get('labels', []),
            title=title,
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Value: %{value}<br>Percentage: %{percent}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            showlegend=True,
            annotations=[
                dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                    showarrow=False, font=dict(size=10)),
                dict(text=f"Total<br>{sum(data.get('values', []))}", x=0.5, y=0.5, 
                    font_size=16, showarrow=False)
            ]
        )
        
        return fig
    
    def _create_bar_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a bar chart"""
        fig = px.bar(
            x=data.get('x', []),
            y=data.get('y', []),
            title=title,
            color=data.get('y', []),
            color_continuous_scale='Blues'
        )
        
        fig.update_traces(
            hovertemplate='<b>%{x}</b><br>Value: %{y}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title=data.get('x_title', 'Category'),
            yaxis_title=data.get('y_title', 'Value'),
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_horizontal_bar_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a horizontal bar chart"""
        fig = px.bar(
            x=data.get('x', []),
            y=data.get('y', []),
            orientation='h',
            title=title,
            color=data.get('x', []),
            color_continuous_scale='Viridis'
        )
        
        fig.update_traces(
            hovertemplate='<b>%{y}</b><br>Value: %{x}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title=data.get('x_title', 'Value'),
            yaxis_title=data.get('y_title', 'Category'),
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_line_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a line chart"""
        fig = px.line(
            x=data.get('x', []),
            y=data.get('y', []),
            title=title,
            markers=True
        )
        
        fig.update_traces(
            line=dict(width=3),
            marker=dict(size=8),
            hovertemplate='<b>%{x}</b><br>Value: %{y}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title=data.get('x_title', 'Time'),
            yaxis_title=data.get('y_title', 'Value'),
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_multi_line_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a multi-line chart"""
        fig = go.Figure()
        
        colors = px.colors.qualitative.Set1
        
        for i, series in enumerate(data):
            fig.add_trace(go.Scatter(
                x=series.get('x', []),
                y=series.get('y', []),
                mode='lines+markers',
                name=series.get('name', f'Series {i+1}'),
                line=dict(width=3, color=colors[i % len(colors)]),
                marker=dict(size=6),
                hovertemplate=f'<b>{series.get("name", f"Series {i+1}")}</b><br>%{{x}}<br>Value: %{{y}}<extra></extra>'
            ))
        
        fig.update_layout(
            title=title,
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title='Time',
            yaxis_title='Value',
            hovermode='x unified',
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_histogram(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a histogram"""
        fig = px.histogram(
            x=data.get('x', []),
            nbins=data.get('nbins', 30),
            title=title,
            color_discrete_sequence=[self.color_palette['primary']]
        )
        
        fig.update_traces(
            hovertemplate='Range: %{x}<br>Count: %{y}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title=data.get('x_title', 'Value'),
            yaxis_title='Frequency',
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_scatter_plot(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a scatter plot"""
        fig = px.scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            size=data.get('size', None),
            color=data.get('color', None),
            title=title,
            hover_name=data.get('hover_name', None)
        )
        
        fig.update_traces(
            hovertemplate='<b>%{hovertext}</b><br>X: %{x}<br>Y: %{y}<extra></extra>'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title=data.get('x_title', 'X Value'),
            yaxis_title=data.get('y_title', 'Y Value'),
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_radar_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a radar chart"""
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=data.get('values', []),
            theta=data.get('categories', []),
            fill='toself',
            name='Values',
            line_color=self.color_palette['primary'],
            fillcolor=f"rgba(31, 119, 180, 0.3)"
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, max(data.get('values', [100])) * 1.1]
                )
            ),
            title=title,
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_gauge_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a gauge chart"""
        value = data.get('value', 0)
        max_value = data.get('max', 100)
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title},
            delta={'reference': max_value * 0.7},
            gauge={
                'axis': {'range': [None, max_value]},
                'bar': {'color': self.color_palette['primary']},
                'steps': [
                    {'range': [0, max_value * 0.3], 'color': "lightgray"},
                    {'range': [max_value * 0.3, max_value * 0.7], 'color': "yellow"},
                    {'range': [max_value * 0.7, max_value], 'color': "green"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': max_value * 0.9
                }
            }
        ))
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_speedometer_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a speedometer-style chart"""
        value = data.get('value', 0)
        max_value = data.get('max', 100)
        zones = data.get('zones', [])
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title},
            gauge={
                'axis': {'range': [None, max_value]},
                'bar': {'color': self._get_zone_color(value, zones)},
                'steps': [{'range': [zone['min'], zone['max']], 'color': zone['color']} for zone in zones],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': max_value * 0.9
                }
            }
        ))
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_metric_cards(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create metric cards visualization"""
        # This is a placeholder - metric cards are better implemented as separate components
        # For now, create a simple bar chart of the metrics
        metrics = list(data.keys())
        values = [data[metric].get('value', 0) for metric in metrics]
        
        fig = px.bar(
            x=metrics,
            y=values,
            title=title,
            color=values,
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            xaxis_title='Metrics',
            yaxis_title='Values',
            annotations=[dict(text=description, x=0.5, y=-0.15, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_heatmap(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a heatmap"""
        fig = px.imshow(
            data.get('z', []),
            x=data.get('x', []),
            y=data.get('y', []),
            title=title,
            color_continuous_scale='RdYlBu_r'
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.1, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_treemap(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a treemap"""
        fig = px.treemap(
            names=data.get('labels', []),
            values=data.get('values', []),
            parents=data.get('parents', []),
            title=title
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.05, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_sunburst_chart(self, data: Dict[str, Any], title: str, description: str, theme: str) -> go.Figure:
        """Create a sunburst chart"""
        fig = px.sunburst(
            names=data.get('labels', []),
            values=data.get('values', []),
            parents=data.get('parents', []),
            title=title
        )
        
        fig.update_layout(
            template=self.chart_themes.get(theme, 'plotly'),
            annotations=[dict(text=description, x=0.5, y=-0.05, xref="paper", yref="paper", 
                            showarrow=False, font=dict(size=10))]
        )
        
        return fig
    
    def _create_error_chart(self, error_message: str) -> go.Figure:
        """Create an error chart when chart generation fails"""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5, y=0.5,
            text=f"Chart Generation Error<br>{error_message}",
            showarrow=False,
            font=dict(size=16, color="red"),
            xref="paper", yref="paper"
        )
        
        fig.update_layout(
            title="Chart Error",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='white'
        )
        
        return fig
    
    def _get_zone_color(self, value: float, zones: List[Dict[str, Any]]) -> str:
        """Get color based on value and zones"""
        for zone in zones:
            if zone['min'] <= value <= zone['max']:
                return zone['color']
        return self.color_palette['primary']
    
    def create_dashboard_layout(self, charts: Dict[str, Any], layout_type: str = 'grid') -> Dict[str, Any]:
        """Create a dashboard layout configuration"""
        layout_config = {
            'type': layout_type,
            'charts': [],
            'created_at': datetime.now().isoformat()
        }
        
        for category, category_charts in charts.items():
            for chart_name, chart_config in category_charts.items():
                layout_config['charts'].append({
                    'id': f"{category}_{chart_name}",
                    'category': category,
                    'name': chart_name,
                    'title': chart_config.get('title', chart_name),
                    'type': chart_config.get('type', 'bar'),
                    'size': self._get_chart_size(chart_config.get('type', 'bar')),
                    'priority': self._get_chart_priority(category, chart_name)
                })
        
        # Sort by priority
        layout_config['charts'].sort(key=lambda x: x['priority'])
        
        return layout_config
    
    def _get_chart_size(self, chart_type: str) -> str:
        """Get recommended size for chart type"""
        size_map = {
            'gauge': 'medium',
            'speedometer': 'medium',
            'pie': 'medium',
            'donut': 'medium',
            'radar': 'large',
            'heatmap': 'large',
            'treemap': 'large',
            'sunburst': 'large',
            'multi_line': 'large',
            'histogram': 'large'
        }
        return size_map.get(chart_type, 'medium')
    
    def _get_chart_priority(self, category: str, chart_name: str) -> int:
        """Get priority for chart placement"""
        priority_map = {
            'executive_metrics': 1,
            'business_formation': 2,
            'economic_indicators': 3,
            'government_spending': 4,
            'property_market': 5,
            'cross_sector': 6,
            'risk_assessment': 7
        }
        return priority_map.get(category, 10)

def create_chart_from_config(chart_config: Dict[str, Any], theme: str = 'default') -> go.Figure:
    """Utility function to create a chart from configuration"""
    generator = InteractiveChartGenerator()
    return generator.create_chart_from_data(chart_config, theme)

def create_dashboard_charts(visual_report: Dict[str, Any], theme: str = 'default') -> Dict[str, go.Figure]:
    """Create all dashboard charts from visual intelligence report"""
    generator = InteractiveChartGenerator()
    charts = {}
    
    if 'visualizations' in visual_report:
        for category, category_charts in visual_report['visualizations'].items():
            charts[category] = {}
            for chart_name, chart_config in category_charts.items():
                try:
                    charts[category][chart_name] = generator.create_chart_from_data(chart_config, theme)
                except Exception as e:
                    logger.error(f"Error creating chart {category}.{chart_name}: {e}")
                    charts[category][chart_name] = generator._create_error_chart(str(e))
    
    return charts