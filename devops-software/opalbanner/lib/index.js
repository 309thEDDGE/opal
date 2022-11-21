import { Widget } from '@lumino/widgets';
function generateContent() {
    return 'OPAL_BANNER_TEXT';
}
const plugin = {
    id: 'contentheader:plugin',
    autoStart: true,
    activate: (app) => {
        console.log('JupyterLab extension contentheader is activated!');
        const widget = new Widget();
        widget.addClass('banner-widget');
        widget.node.textContent = generateContent();
        const rootLayout = app.shell.layout;
        rootLayout.insertWidget(0, widget);
    },
};
export default plugin;
