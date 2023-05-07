db.createUser(
        {
            user: "dap",
            pwd: "dap",
            roles: [
                {
                    role: "readWrite",
                    db: "dap"
                }
            ]
        }
);