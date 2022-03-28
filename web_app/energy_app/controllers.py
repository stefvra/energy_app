
from flask import render_template, url_for, redirect, request


class Dashboard_Controller():

    def __init__(self, models, templates, main_template, error_template='error.html'):
        self.models = models
        self.templates = templates
        self.main_template = main_template
        self.error_template = error_template
    

    def control_get(self, request):
        data = []
        for model, template in zip(self.models, self.templates):
            
            #model_data = model.get(request)
            #model_data['template'] = template
            
            try:
                model_data = model.get(request)
                model_data['template'] = template
            except:
                model_data = dict()
                model_data['template'] = self.error_template
                model_data['message'] = f'Unable to get data for {model.title}...'
            data.append(model_data)
        return render_template(self.main_template, data=data)


    def control_post(self, request):

        for model in self.models:
            model.post(request)
        return redirect(url_for('main'))