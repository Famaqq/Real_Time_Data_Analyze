__author__ = 'jianxun'

import config


def generate_chart_content(title, subtitle, chart_id, data_list):
        result = {}

        result['chart'] = {'renderTo': chart_id, 'type': 'spline', 'marginRight': 130, 'marginBottom': 25}

        result['title'] = {'text': title, 'x': -20}
        result['subtitle'] = {'text': subtitle, 'x': -20}

        times = []
        series = {}
        for data in data_list:
            times.append(data['time'].strftime(config.TIME_FORMAT))
            for attr in data['attrs']:
                if isinstance(data['attrs'][attr], int) or isinstance(data['attrs'][attr], float):
                    if attr in series:
                        series[attr].append(data['attrs'][attr])
                    else:
                        series[attr] = [data['attrs'][attr]]

        result['series'] = []
        for attr in series:
            result['series'].append({'name': attr, 'data': series[attr]})
        result['xAxis'] = {'categories': times}
        result['yAxis'] = {'title': {'text': 'number'}, 'plotLines': [{'value': 0, 'width': 1, 'color': '#808080'}]}
        result['legend'] = {
            'layout': 'vertical',
            'align': 'right',
            'verticalAlign': 'middle',
            'borderWidth': 0
        },
        return result
