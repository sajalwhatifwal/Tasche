const types = {
    HTTP_REQUEST: {
        name: 'HTTP_REQUEST',
        requiredMeta: [
            'method',
            'url'
        ]
    }
};

const supportedTypes = [
    types.HTTP_REQUEST.name
]

const invoker = (promise) => {
    return promise
    .then((data) => {
        return [null, data];
    })
    .catch((err) => {
        return [err, null];
    });
};

module.exports = {
    types,
    invoker,
    supportedTypes
}